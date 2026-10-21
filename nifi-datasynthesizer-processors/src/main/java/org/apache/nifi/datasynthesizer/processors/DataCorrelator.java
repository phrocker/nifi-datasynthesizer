/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.datasynthesizer.processors;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.datasynthesizer.SamplerBase;
import org.apache.nifi.datasynthesizer.llm.TextCompletionContext;
import org.apache.nifi.datasynthesizer.processors.chaos.ChaosProfile;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"data-synthesis", "synthetic", "record", "correlate", "join", "enrich"})
@CapabilityDescription("Enriches each incoming record with generated data. For every input record the schema is sampled "
        + "'Correlations Per Record' times and each sample is placed into a copy of the input record under the "
        + "'Record Schema Key' field. Enriched records go to 'success'; the generated pieces alone go to "
        + "'correlation' so the two streams can be joined downstream.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Number of records written"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type reported by the record writer")})
public class DataCorrelator extends DataSynthesizerBase {

    public static final PropertyDescriptor SCHEMA_KEY = new PropertyDescriptor.Builder()
            .name("schema-key")
            .displayName("Record Schema Key")
            .description("Name of the field that receives the generated data in each enriched record.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor SELECT_OBJECT = new PropertyDescriptor.Builder()
            .name("select-object")
            .displayName("Select Object")
            .description("When true, only the sampled field named by 'Record Schema Key' is emplaced; otherwise the whole "
                    + "sampled object is.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor CORRELATIONS_PER_RECORD = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(RECORD_COUNT)
            .displayName("Correlations Per Record")
            .description("Number of enriched copies to produce for each input record.")
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Controller service used to read incoming records.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The incoming FlowFile")
            .build();

    public static final Relationship REL_CORRELATION = new Relationship.Builder()
            .name("correlation")
            .description("The generated pieces on their own, one record per correlation")
            .build();

    private static final PropertyDescriptor CORRELATOR_SCHEMA = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SCHEMA)
            .description("log-synth schema sampled for each correlation, or a schema name when a Schema Lookup Service is set.")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = withChaos(List.of(
            CORRELATOR_SCHEMA, SCHEMA_KEY, SELECT_OBJECT, CORRELATIONS_PER_RECORD, RECORD_READER, RECORD_WRITER,
            SCHEMA_LOOKUP_SERVICE, TEXT_GENERATION_SERVICE));
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL, REL_CORRELATION, REL_CHAOS, REL_INVALID);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile input = session.get();
        if (input == null) {
            return;
        }
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String key = context.getProperty(SCHEMA_KEY).getValue();
        final boolean selectObject = context.getProperty(SELECT_OBJECT).asBoolean();
        final int perRecord = context.getProperty(CORRELATIONS_PER_RECORD).asInteger();

        final List<FlowFile> created = new ArrayList<>();
        try (TextCompletionContext.Scope ignored = bindTextGeneration()) {
            final SamplerBase sampler = configuredSampler();
            final ChaosProfile.Batch batch = chaos.batch(() -> next(sampler));

            // Buffer the enriched records so tampered ones can be routed separately and the schema is known up front.
            final List<Record> clean = new ArrayList<>();
            final List<Record> tampered = new ArrayList<>();
            final List<JsonNode> pieces = new ArrayList<>();
            RecordSchema mergedSchema = null;
            DataType pieceType = null;

            try (InputStream in = session.read(input);
                 RecordReader reader = readerFactory.createRecordReader(input, in, getLogger())) {
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    for (int i = 0; i < perRecord; i++) {
                        for (final JsonNode sampled : batch.next()) {
                            final boolean isTampered = ChaosProfile.isTampered(sampled);
                            JsonNode piece = sampled;
                            if (selectObject) {
                                piece = sampled.get(key);
                                if (piece == null) {
                                    throw new ProcessException("Sampled object has no field named '" + key + "'");
                                }
                                if (chaos.isEnabled() && piece instanceof ObjectNode object) {
                                    object.set(ChaosProfile.DEFAULT_ID_FIELD, sampled.get(ChaosProfile.DEFAULT_ID_FIELD));
                                }
                            }
                            if (mergedSchema == null) {
                                pieceType = JsonRecords.inferDataType(key, piece);
                                mergedSchema = JsonRecords.withField(reader.getSchema(), key, pieceType);
                                if (chaos.isEnabled()) {
                                    mergedSchema = JsonRecords.withField(mergedSchema, ChaosProfile.DEFAULT_ID_FIELD, RecordFieldType.STRING.getDataType());
                                }
                            }
                            final Map<String, Object> values = new HashMap<>(record.toMap());
                            values.put(key, JsonRecords.toValue(piece, pieceType));
                            if (chaos.isEnabled()) {
                                values.put(ChaosProfile.DEFAULT_ID_FIELD, isTampered ? sampled.get(ChaosProfile.DEFAULT_ID_FIELD).asText() : null);
                            }
                            (isTampered && chaos.isSeparate() ? tampered : clean).add(new MapRecord(mergedSchema, values, false, false));
                            pieces.add(piece);
                        }
                    }
                }
            } catch (final SchemaNotFoundException | MalformedRecordException e) {
                throw new ProcessException("Unable to read incoming records", e);
            }

            if (mergedSchema == null) {
                session.transfer(input, REL_ORIGINAL);
                return;
            }
            // Write everything first; transfer only once nothing can fail.
            final Map<FlowFile, Relationship> transfers = new java.util.LinkedHashMap<>();
            final FlowFile enriched = writeRecordList(session, context, input, mergedSchema, clean);
            created.add(enriched);
            transfers.put(enriched, REL_SUCCESS);
            if (!tampered.isEmpty()) {
                final FlowFile invalid = writeRecordList(session, context, input, mergedSchema, tampered);
                created.add(invalid);
                transfers.put(invalid, REL_INVALID);
            }
            if (!pieces.isEmpty()) {
                final java.util.Iterator<JsonNode> pieceIterator = pieces.iterator();
                final FlowFile correlation = writeWithoutChaos(session, context, input, pieceIterator::next, pieces.size());
                created.add(correlation);
                transfers.put(correlation, REL_CORRELATION);
            }
            if (chaos.isEnabled() && !batch.manifest().isEmpty()) {
                final FlowFile manifest = writeManifest(session, input, batch.manifest());
                created.add(manifest);
                transfers.put(manifest, REL_CHAOS);
            }
            transfers.forEach(session::transfer);
            session.transfer(input, REL_ORIGINAL);
        } catch (final IOException | RuntimeException e) {
            getLogger().error("Failed to correlate records for {}", input, e);
            created.forEach(session::remove);
            session.transfer(session.penalize(input), REL_FAILURE);
        }
    }
}
