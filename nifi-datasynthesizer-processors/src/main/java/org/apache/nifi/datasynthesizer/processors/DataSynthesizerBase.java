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
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.HashMap;
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

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

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

    /**
     * Writes {@code count} records produced by {@code supplier} into a new FlowFile (derived from {@code parent} when
     * not null) and returns it with record.count and mime.type attributes set.
     */
    protected FlowFile writeRecords(final ProcessSession session, final ProcessContext context, final FlowFile parent,
                                    final Supplier<JsonNode> supplier, final int count) throws IOException {
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final JsonNode first = supplier.get();
        final RecordSchema schema = JsonRecords.inferSchema(first);
        final Map<String, String> variables = parent == null ? Map.of() : parent.getAttributes();
        final Map<String, String> attributes = new HashMap<>();

        FlowFile flowFile = parent == null ? session.create() : session.create(parent);
        flowFile = session.write(flowFile, out -> {
            try {
                final RecordSchema writeSchema = writerFactory.getSchema(variables, schema);
                try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, variables)) {
                    writer.beginRecordSet();
                    writer.write(JsonRecords.toRecord(first, schema));
                    for (int i = 1; i < count; i++) {
                        writer.write(JsonRecords.toRecord(supplier.get(), schema));
                    }
                    final WriteResult result = writer.finishRecordSet();
                    attributes.putAll(result.getAttributes());
                    attributes.put("record.count", String.valueOf(result.getRecordCount()));
                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                }
            } catch (final SchemaNotFoundException e) {
                throw new ProcessException("Record writer could not resolve a schema", e);
            }
        });
        return session.putAllAttributes(flowFile, attributes);
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
