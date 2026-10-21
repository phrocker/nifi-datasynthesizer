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
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.mapr.synth.samplers.SchemaSampler;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.datasynthesizer.SamplerBase;
import org.apache.nifi.datasynthesizer.llm.TextCompletion;
import org.apache.nifi.datasynthesizer.llm.TextCompletionContext;
import org.apache.nifi.datasynthesizer.llm.TextCompletionException;
import org.apache.nifi.datasynthesizer.processors.chaos.ChaosProfile;
import org.apache.nifi.datasynthesizer.services.SchemaLookupService;
import org.apache.nifi.datasynthesizer.services.TextGenerationRequest;
import org.apache.nifi.datasynthesizer.services.TextGenerationService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Shared plumbing for processors that emit log-synth samples as NiFi records.
 */
public abstract class DataSynthesizerBase extends AbstractProcessor {

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("schema")
            .displayName("Record Schema")
            .description("A log-synth schema (JSON array of field samplers). When a Schema Lookup Service is configured "
                    + "this is the name of the schema to look up instead. When neither is set, the content of each "
                    + "incoming FlowFile is used as the schema.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor SCHEMA_LOOKUP_SERVICE = new PropertyDescriptor.Builder()
            .name("schema-lookup")
            .displayName("Schema Lookup Service")
            .description("Controller service holding named schemas shared by several processors.")
            .identifiesControllerService(SchemaLookupService.class)
            .required(false)
            .build();

    public static final PropertyDescriptor RECORD_COUNT = new PropertyDescriptor.Builder()
            .name("record-count")
            .displayName("Record Count")
            .description("Number of records to create per FlowFile.")
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Controller service used to write the generated records.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor TEXT_GENERATION_SERVICE = new PropertyDescriptor.Builder()
            .name("text-generation-service")
            .displayName("Text Generation Service")
            .description("Optional LLM-backed service. Required when the schema contains a field of class 'llm'.")
            .identifiesControllerService(TextGenerationService.class)
            .required(false)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Generated records")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Input FlowFiles for which records could not be generated")
            .build();

    public static final Relationship REL_CHAOS = new Relationship.Builder()
            .name("chaos")
            .description("Manifest of injected faults (one record per fault) when Chaos Enabled is true. "
                    + "Auto-terminate when chaos is not in use.")
            .autoTerminateDefault(true)
            .build();

    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("Tampered records when Chaos Routing is 'Separate': the negative path of a flow under test. "
                    + "Not a failure. Auto-terminate when chaos is not in use.")
            .autoTerminateDefault(true)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_CHAOS, REL_INVALID);

    /** Appends the chaos profile properties to a processor's own list. */
    protected static List<PropertyDescriptor> withChaos(final List<PropertyDescriptor> own) {
        final List<PropertyDescriptor> all = new ArrayList<>(own);
        all.addAll(ChaosProfile.PROPERTIES);
        return List.copyOf(all);
    }

    protected volatile ChaosProfile chaos = ChaosProfile.OFF;

    /** Schema text fixed by configuration or by a subclass; null means it comes from the FlowFile or lookup service. */
    protected volatile String definedSchema;
    protected volatile SchemaLookupService schemaLookupService;
    protected volatile TextGenerationService textGenerationService;

    private volatile ThreadLocal<SamplerBase> samplers = new ThreadLocal<>();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void configureBase(final ProcessContext context) {
        samplers = new ThreadLocal<>();
        chaos = ChaosProfile.from(context);
        schemaLookupService = context.getProperty(SCHEMA_LOOKUP_SERVICE) != null && context.getProperty(SCHEMA_LOOKUP_SERVICE).isSet()
                ? context.getProperty(SCHEMA_LOOKUP_SERVICE).asControllerService(SchemaLookupService.class)
                : null;
        textGenerationService = context.getProperty(TEXT_GENERATION_SERVICE) != null && context.getProperty(TEXT_GENERATION_SERVICE).isSet()
                ? context.getProperty(TEXT_GENERATION_SERVICE).asControllerService(TextGenerationService.class)
                : null;
        if (context.getProperty(SCHEMA) != null && context.getProperty(SCHEMA).isSet()) {
            definedSchema = context.getProperty(SCHEMA).getValue();
        }
    }

    /** Parses a log-synth schema, accepting either a JSON array of samplers or a single sampler object. */
    public static SchemaSampler parseSchema(final String schema) throws IOException {
        try {
            return new SchemaSampler(schema);
        } catch (final MismatchedInputException e) {
            return new SchemaSampler("[" + schema + "]");
        }
    }

    /** Returns the sampler for the configured schema, building it once per thread. */
    protected SamplerBase configuredSampler() throws IOException {
        SamplerBase sampler = samplers.get();
        if (sampler != null) {
            return sampler;
        }
        final String schema = definedSchema;
        if (schemaLookupService != null && schema != null && schemaLookupService.hasSchema(schema)) {
            sampler = new LookupSampler(schemaLookupService, schema);
        } else if (schema != null) {
            sampler = parseSchema(schema);
        } else {
            throw new IOException("No schema configured");
        }
        samplers.set(sampler);
        return sampler;
    }

    protected boolean hasConfiguredSchema() {
        return definedSchema != null;
    }

    /** Resets cached samplers, for subclasses that rebuild their schema. */
    protected void resetSamplers() {
        samplers = new ThreadLocal<>();
    }

    protected TextCompletionContext.Scope bindTextGeneration() {
        final TextGenerationService service = textGenerationService;
        if (service == null) {
            return TextCompletionContext.use(NO_PROVIDER);
        }
        return TextCompletionContext.use((system, prompt, maxTokens, temperature) -> {
            try {
                return service.generate(new TextGenerationRequest(system, prompt, maxTokens, temperature));
            } catch (final IOException e) {
                throw new TextCompletionException("Text generation failed: " + e.getMessage(), e);
            }
        });
    }

    private static final TextCompletion NO_PROVIDER = (system, prompt, maxTokens, temperature) -> {
        throw new TextCompletionException("Schema uses an 'llm' sampler but no Text Generation Service is configured on the processor");
    };

    protected static JsonNode next(final SamplerBase sampler) {
        return sampler.nextSample().orElseThrow(() -> new ProcessException("Sampler produced no record"));
    }

    /** Number of leading samples the output schema is inferred from, so shape variance across samples is captured. */
    static final int INFERENCE_SAMPLES = 10;

    private static final com.fasterxml.jackson.databind.ObjectMapper MANIFEST_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();

    /**
     * Writes {@code count} records produced by {@code supplier} into a new FlowFile (derived from {@code parent} when
     * not null) and returns it with record.count and mime.type attributes set. The schema is inferred from the first
     * {@link #INFERENCE_SAMPLES} records. When chaos is enabled, tampered records may instead be routed to
     * {@link #REL_INVALID} and a fault manifest is routed to {@link #REL_CHAOS}. On failure every FlowFile created
     * here is removed before the exception propagates.
     */
    protected FlowFile writeRecords(final ProcessSession session, final ProcessContext context, final FlowFile parent,
                                    final Supplier<JsonNode> supplier, final int count) throws IOException {
        final ChaosProfile.Batch batch = chaos.batch(supplier);
        FlowFile output = null;
        FlowFile invalid = null;
        FlowFile manifest = null;
        try {
            if (chaos.isSeparate()) {
                // Draw the whole batch so tampered and clean records can be written to different FlowFiles.
                final List<JsonNode> clean = new ArrayList<>();
                final List<JsonNode> tampered = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    for (final JsonNode node : batch.next()) {
                        (ChaosProfile.isTampered(node) ? tampered : clean).add(node);
                    }
                }
                output = writeList(session, context, parent, clean, batch::exemplar);
                if (!tampered.isEmpty()) {
                    invalid = writeList(session, context, parent, tampered, batch::exemplar);
                }
            } else {
                output = writeNodes(session, context, parent, () -> batch.next().iterator(), count, batch::exemplar);
            }
            if (chaos.isEnabled() && !batch.manifest().isEmpty()) {
                manifest = writeManifest(session, parent, batch.manifest());
            }
        } catch (final IOException | RuntimeException e) {
            for (final FlowFile created : new FlowFile[] {output, invalid, manifest}) {
                if (created != null) {
                    session.remove(created);
                }
            }
            throw e;
        }
        if (invalid != null) {
            session.transfer(invalid, REL_INVALID);
        }
        if (manifest != null) {
            session.transfer(manifest, REL_CHAOS);
        }
        return output;
    }

    /**
     * Writes the chaos manifest as newline-delimited JSON. It deliberately bypasses the configured Record Writer:
     * a writer bound to the payload's schema (for example Avro with an explicit schema) could not serialize it.
     */
    protected FlowFile writeManifest(final ProcessSession session, final FlowFile parent, final List<ChaosProfile.ManifestEntry> entries) {
        FlowFile flowFile = parent == null ? session.create() : session.create(parent);
        try {
            flowFile = session.write(flowFile, out -> {
                for (final ChaosProfile.ManifestEntry entry : entries) {
                    out.write(MANIFEST_MAPPER.writeValueAsBytes(entry.toJson()));
                    out.write('\n');
                }
            });
        } catch (final RuntimeException e) {
            session.remove(flowFile);
            throw e;
        }
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("record.count", String.valueOf(entries.size()));
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/x-ndjson");
        attributes.put("chaos.manifest", "true");
        return session.putAllAttributes(flowFile, attributes);
    }

    /** Writes a fully drawn list; {@code exemplar} lends its shape to the schema when the list is empty. */
    private FlowFile writeList(final ProcessSession session, final ProcessContext context, final FlowFile parent,
                               final List<JsonNode> nodes, final Supplier<JsonNode> exemplar) throws IOException {
        final java.util.Iterator<JsonNode> iterator = nodes.iterator();
        return writeNodes(session, context, parent, () -> List.of(iterator.next()).iterator(), nodes.size(), exemplar);
    }

    /**
     * Writes records into a new FlowFile. {@code draw} is invoked {@code count} times and each call may yield zero
     * or more records (chaos may drop or duplicate). The schema is inferred from the first records drawn; when no
     * record survives, {@code exemplar} supplies the schema and an empty record set is written.
     */
    private FlowFile writeNodes(final ProcessSession session, final ProcessContext context, final FlowFile parent,
                                final Supplier<java.util.Iterator<JsonNode>> draw, final int count,
                                final Supplier<JsonNode> exemplar) throws IOException {
        final List<JsonNode> head = new ArrayList<>();
        int drawn = 0;
        while (drawn < count && head.size() < INFERENCE_SAMPLES) {
            draw.get().forEachRemaining(head::add);
            drawn++;
        }
        while (drawn < count && head.isEmpty()) {
            draw.get().forEachRemaining(head::add);
            drawn++;
        }
        final int remaining = count - drawn;
        final RecordSchema schema;
        if (head.isEmpty()) {
            final JsonNode shape = exemplar == null ? null : exemplar.get();
            if (shape == null) {
                throw new IOException("No records were produced");
            }
            schema = extendForChaos(JsonRecords.inferSchema(shape));
        } else {
            schema = extendForChaos(JsonRecords.inferSchema(head.toArray(new JsonNode[0])));
        }
        final java.util.Iterator<JsonNode> headIterator = head.iterator();
        return writeWithSchema(session, context, parent, schema, () -> {
            if (headIterator.hasNext()) {
                return List.of(headIterator.next()).iterator();
            }
            return null;
        }, remaining, draw);
    }

    /** Writes records straight through, with no chaos batch: used for outputs derived from already-chaosed data. */
    protected FlowFile writeWithoutChaos(final ProcessSession session, final ProcessContext context, final FlowFile parent,
                                         final Supplier<JsonNode> supplier, final int count) throws IOException {
        return writeNodes(session, context, parent, () -> List.of(supplier.get()).iterator(), count, null);
    }

    /** Writes already converted records with a known schema. */
    protected FlowFile writeRecordList(final ProcessSession session, final ProcessContext context, final FlowFile parent,
                                       final RecordSchema schema, final List<Record> records) throws IOException {
        return write(session, context, parent, schema, writer -> {
            for (final Record record : records) {
                writer.write(record);
            }
        });
    }

    private FlowFile writeWithSchema(final ProcessSession session, final ProcessContext context, final FlowFile parent,
                                     final RecordSchema schema, final Supplier<java.util.Iterator<JsonNode>> head,
                                     final int remaining, final Supplier<java.util.Iterator<JsonNode>> draw) throws IOException {
        return write(session, context, parent, schema, writer -> {
            java.util.Iterator<JsonNode> nodes;
            while ((nodes = head.get()) != null) {
                while (nodes.hasNext()) {
                    writer.write(JsonRecords.toRecord(nodes.next(), schema));
                }
            }
            for (int i = 0; i < remaining; i++) {
                nodes = draw.get();
                while (nodes.hasNext()) {
                    writer.write(JsonRecords.toRecord(nodes.next(), schema));
                }
            }
        });
    }

    @FunctionalInterface
    private interface RecordBody {
        void write(RecordSetWriter writer) throws IOException;
    }

    private FlowFile write(final ProcessSession session, final ProcessContext context, final FlowFile parent,
                           final RecordSchema schema, final RecordBody body) throws IOException {
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final Map<String, String> variables = parent == null ? Map.of() : parent.getAttributes();
        final Map<String, String> attributes = new HashMap<>();
        FlowFile flowFile = parent == null ? session.create() : session.create(parent);
        try {
            flowFile = session.write(flowFile, out -> {
                try {
                    final RecordSchema writeSchema = writerFactory.getSchema(variables, schema);
                    try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, variables)) {
                        writer.beginRecordSet();
                        body.write(writer);
                        final WriteResult result = writer.finishRecordSet();
                        attributes.putAll(result.getAttributes());
                        attributes.put("record.count", String.valueOf(result.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    }
                } catch (final SchemaNotFoundException e) {
                    throw new ProcessException("Record writer could not resolve a schema", e);
                }
            });
        } catch (final RuntimeException e) {
            session.remove(flowFile);
            throw e;
        }
        return session.putAllAttributes(flowFile, attributes);
    }

    /**
     * Prepares an inferred schema for chaos: declares the chaos fields so tagged and drifted records survive the
     * record writer, and widens scalar fields to a choice with string so a TYPE fault that appears after the
     * inference window can still be serialized by schema-enforcing writers.
     */
    protected RecordSchema extendForChaos(final RecordSchema inferred) {
        if (!chaos.isEnabled()) {
            return inferred;
        }
        final List<org.apache.nifi.serialization.record.RecordField> fields = new ArrayList<>();
        for (final org.apache.nifi.serialization.record.RecordField field : inferred.getFields()) {
            fields.add(new org.apache.nifi.serialization.record.RecordField(field.getFieldName(), widen(field.getDataType()), true));
        }
        RecordSchema schema = new org.apache.nifi.serialization.SimpleRecordSchema(fields);
        if (schema.getField(ChaosProfile.DEFAULT_ID_FIELD).isEmpty()) {
            schema = JsonRecords.withField(schema, ChaosProfile.DEFAULT_ID_FIELD,
                    org.apache.nifi.serialization.record.RecordFieldType.STRING.getDataType());
        }
        if (chaos.mayAddUnexpectedField() && schema.getField(ChaosProfile.UNEXPECTED_FIELD).isEmpty()) {
            schema = JsonRecords.withField(schema, ChaosProfile.UNEXPECTED_FIELD,
                    org.apache.nifi.serialization.record.RecordFieldType.STRING.getDataType());
        }
        return schema;
    }

    private static org.apache.nifi.serialization.record.DataType widen(final org.apache.nifi.serialization.record.DataType type) {
        final org.apache.nifi.serialization.record.RecordFieldType fieldType = type.getFieldType();
        return switch (fieldType) {
            case STRING, RECORD, ARRAY, MAP, CHOICE, ENUM -> type;
            default -> org.apache.nifi.serialization.record.RecordFieldType.CHOICE.getChoiceDataType(
                    type, org.apache.nifi.serialization.record.RecordFieldType.STRING.getDataType());
        };
    }

    /** Default behaviour for source-style processors: emit Record Count records from the configured schema. */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int recordCount = context.getProperty(RECORD_COUNT).asInteger();
        try (TextCompletionContext.Scope ignored = bindTextGeneration()) {
            final SamplerBase sampler = configuredSampler();
            final FlowFile flowFile = writeRecords(session, context, null, () -> next(sampler), recordCount);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final IOException | RuntimeException e) {
            getLogger().error("Failed to synthesize records", e);
            context.yield();
            throw new ProcessException("Failed to synthesize records: " + e.getMessage(), e);
        }
    }
}
