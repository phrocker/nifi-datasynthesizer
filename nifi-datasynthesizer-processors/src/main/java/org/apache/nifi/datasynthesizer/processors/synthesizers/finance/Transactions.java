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
package org.apache.nifi.datasynthesizer.processors.synthesizers.finance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mapr.synth.samplers.SchemaSampler;
import com.mapr.synth.samplers.ZipSampler;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.datasynthesizer.processors.JsonRecords;
import org.apache.nifi.datasynthesizer.processors.synthesizers.iot.IotData;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
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
import java.util.concurrent.ThreadLocalRandom;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"data-synthesis", "synthetic", "record", "finance", "transaction", "credit-card"})
@CapabilityDescription("Generates credit card transactions against a fixed pool of merchant terminals, each with a real "
        + "US address. Without input, each record is a 'transactions' array. With input, every incoming record is "
        + "enriched with a 'transactions' array, so an upstream person generator yields card holders with purchases.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Number of records written"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type reported by the record writer")})
public class Transactions extends DataSynthesizerBase {

    public static final String TRANSACTIONS_FIELD = "transactions";

    public static final PropertyDescriptor TERMINALS_TO_GENERATE = new PropertyDescriptor.Builder()
            .name("terminal-generation-count")
            .displayName("Terminal Count")
            .description("Size of the merchant terminal pool shared by all transactions.")
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Controller service used to read incoming records when input is connected.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(false)
            .build();

    public static final PropertyDescriptor GENERATE_INVALID_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("generate-invalid-transactions")
            .displayName("Generate Invalid Transactions")
            .description("When true, amounts range up to 50000 so that outlier transactions appear.")
            .defaultValue("false")
            .allowableValues("true", "false")
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("max-transactions")
            .displayName("Max Transactions")
            .description("Maximum number of transactions per record.")
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor FIXED_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("fixed-transaction-count")
            .displayName("Fixed Transaction Count")
            .description("When true every record gets exactly Max Transactions; otherwise a random count up to it.")
            .defaultValue("false")
            .allowableValues("true", "false")
            .required(true)
            .build();

    public static final PropertyDescriptor ALLOW_ZERO_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("allow-zero-transactions")
            .displayName("Allow Zero Transactions")
            .description("When counts are random, whether a record may have no transactions at all.")
            .defaultValue("false")
            .allowableValues("true", "false")
            .required(true)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The incoming FlowFile, when input is connected")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = withChaos(List.of(
            MAX_TRANSACTIONS, FIXED_TRANSACTIONS, ALLOW_ZERO_TRANSACTIONS, TERMINALS_TO_GENERATE,
            GENERATE_INVALID_TRANSACTIONS, RECORD_COUNT, RECORD_WRITER, RECORD_READER_FACTORY));
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL, REL_CHAOS, REL_INVALID);

    private volatile List<ObjectNode> terminals = List.of();
    private volatile ThreadLocal<SchemaSampler> transactionSamplers = new ThreadLocal<>();
    private volatile String transactionSchema;
    private volatile int maxTransactions;
    private volatile boolean fixed;
    private volatile boolean allowZero;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        maxTransactions = context.getProperty(MAX_TRANSACTIONS).asInteger();
        fixed = context.getProperty(FIXED_TRANSACTIONS).asBoolean();
        allowZero = context.getProperty(ALLOW_ZERO_TRANSACTIONS).asBoolean();
        transactionSchema = IotData.readResource(context.getProperty(GENERATE_INVALID_TRANSACTIONS).asBoolean()
                ? "/generators/transaction.json" : "/generators/valid_transaction.json");
        transactionSamplers = new ThreadLocal<>();

        final SchemaSampler terminalSampler = parseSchema(IotData.readResource("/generators/terminal.json"));
        final ZipSampler zipSampler = new ZipSampler();
        zipSampler.setUseAddress("true");
        final int count = context.getProperty(TERMINALS_TO_GENERATE).asInteger();
        final List<ObjectNode> pool = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final JsonNode location = zipSampler.sample();
            final ObjectNode terminal = new ObjectNode(JsonNodeFactory.instance);
            terminal.set("terminal_id", terminalSampler.sample().get("terminal_id"));
            terminal.set("terminal_address", location.get("address"));
            terminal.set("terminal_city", location.get("city"));
            terminal.set("terminal_state", location.get("state"));
            terminal.set("terminal_zip", location.get("zip"));
            pool.add(terminal);
        }
        terminals = List.copyOf(pool);
    }

    private SchemaSampler transactionSampler() throws IOException {
        SchemaSampler sampler = transactionSamplers.get();
        if (sampler == null) {
            sampler = parseSchema(transactionSchema);
            transactionSamplers.set(sampler);
        }
        return sampler;
    }

    int transactionCount() {
        if (fixed) {
            return maxTransactions;
        }
        final int count = ThreadLocalRandom.current().nextInt(maxTransactions + 1);
        return count == 0 && !allowZero ? 1 : count;
    }

    ObjectNode transaction() throws IOException {
        final ObjectNode transaction = (ObjectNode) transactionSampler().sample();
        final List<ObjectNode> pool = terminals;
        transaction.setAll(pool.get(ThreadLocalRandom.current().nextInt(pool.size())));
        return transaction;
    }

    ArrayNode transactions(final int count) throws IOException {
        final ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
        for (int i = 0; i < count; i++) {
            array.add(transaction());
        }
        return array;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile input = session.get();
        if (input == null) {
            if (context.hasIncomingConnection()) {
                return;
            }
            generateStandalone(context, session);
            return;
        }
        if (!context.getProperty(RECORD_READER_FACTORY).isSet()) {
            getLogger().error("A Record Reader is required to enrich incoming records; routing {} to failure", input);
            session.transfer(session.penalize(input), REL_FAILURE);
            return;
        }
        enrich(context, session, input);
    }

    private void generateStandalone(final ProcessContext context, final ProcessSession session) {
        final int recordCount = context.getProperty(RECORD_COUNT).asInteger();
        try {
            final FlowFile flowFile = writeRecords(session, context, null, () -> {
                try {
                    final ObjectNode record = new ObjectNode(JsonNodeFactory.instance);
                    record.set(TRANSACTIONS_FIELD, transactions(transactionCount()));
                    return record;
                } catch (final IOException e) {
                    throw new ProcessException(e);
                }
            }, recordCount);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final IOException | RuntimeException e) {
            getLogger().error("Failed to synthesize transactions", e);
            context.yield();
            throw new ProcessException("Failed to synthesize transactions: " + e.getMessage(), e);
        }
    }

    private void enrich(final ProcessContext context, final ProcessSession session, final FlowFile input) {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        FlowFile output = session.create(input);
        final Map<String, String> attributes = new HashMap<>();
        try {
            // Infer the array type from a populated sample so an empty first array does not degrade the schema.
            final ObjectNode probe = new ObjectNode(JsonNodeFactory.instance);
            probe.set(TRANSACTIONS_FIELD, transactions(1));
            final DataType arrayType = JsonRecords.inferSchema(probe).getDataType(TRANSACTIONS_FIELD).orElseThrow();

            output = session.write(output, out -> {
                try (InputStream in = session.read(input);
                     RecordReader reader = readerFactory.createRecordReader(input, in, getLogger())) {
                    final RecordSchema schema = JsonRecords.withField(reader.getSchema(), TRANSACTIONS_FIELD, arrayType);
                    final RecordSchema writeSchema = writerFactory.getSchema(input.getAttributes(), schema);
                    try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, input.getAttributes())) {
                        writer.beginRecordSet();
                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            final Map<String, Object> values = new HashMap<>(record.toMap());
                            values.put(TRANSACTIONS_FIELD, JsonRecords.toValue(transactions(transactionCount()), arrayType));
                            writer.write(new MapRecord(schema, values, false, false));
                        }
                        final WriteResult result = writer.finishRecordSet();
                        attributes.putAll(result.getAttributes());
                        attributes.put("record.count", String.valueOf(result.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    }
                } catch (final SchemaNotFoundException | MalformedRecordException e) {
                    throw new ProcessException("Unable to read incoming records", e);
                }
            });
            output = session.putAllAttributes(output, attributes);
            session.transfer(output, REL_SUCCESS);
            session.transfer(input, REL_ORIGINAL);
        } catch (final IOException | RuntimeException e) {
            getLogger().error("Failed to add transactions to {}", input, e);
            session.remove(output);
            session.transfer(session.penalize(input), REL_FAILURE);
        }
    }
}
