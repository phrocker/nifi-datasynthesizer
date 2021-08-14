package org.apache.nifi.datasynthesizer.processors.synthesizers.finance;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.javafaker.Faker;
import com.mapr.synth.samplers.SchemaSampler;
import com.mapr.synth.samplers.ZipSampler;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.data.JsonWriter;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.JsonSchemaInference;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"data-synthesis", "record"})
public class Transactions extends DataSynthesizerBase {

    protected static final PropertyDescriptor TERMINALS_TO_GENERATE = new PropertyDescriptor.Builder()
            .name("terminal-generation-count")
            .displayName("Terminal Generation Count")
            .description("Specify the number of terminals to generate to be used, globally")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();

    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(false)
            .build();

    protected static final PropertyDescriptor GENERATE_INVALID_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("generate-invalid-transactions")
            .displayName("Generate Invalid Transactions")
            .description("True will generate invalid transactions")
            .defaultValue("False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .build();

    protected static final PropertyDescriptor MAX_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("max-transactions")
            .displayName("Max Transactions")
            .description("The maximum number of transactions per person")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("1")
            .required(false)
            .build();

    protected static final PropertyDescriptor ALLOW_ZERO_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("allow-zero-transactions")
            .displayName("Allow zero transactions")
            .description("The maximum number of transactions per person")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .required(false)
            .build();


    protected static final PropertyDescriptor FIXED_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("fixed-transaction-count")
            .displayName("Fixed Transaction Count")
            .description("If true then max transactions will be determined as a fixed number, not an upper bound")
            .defaultValue("False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();

    private Map<String, org.codehaus.jackson.JsonNode> terminalMap = new HashMap<>();

    Faker faker = new Faker();

    protected ThreadLocal<SchemaSampler> sampler = new ThreadLocal<>();

    protected ThreadLocal<SchemaSampler> transactionGenerator = new ThreadLocal<>();

    protected boolean isFixed = false;

    protected int maxTransactions = 1;

    private Random rand = new Random();

    protected RecordReaderFactory recordParserFactory;

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original data record if an input is provided.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Data could not be synthesized")
            .build();

    public static final Relationship REL_CORRELATION = new Relationship.Builder()
            .name("correlation")
            .description("Correlation relationship")
            .build();

    public Transactions(){

    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        rels.add(REL_ORIGINAL);
        return rels;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        sampler = new ThreadLocal<>();
        int terminals = 1000;
        if (context.getProperty(TERMINALS_TO_GENERATE).isSet()){
            terminals = context.getProperty(TERMINALS_TO_GENERATE).asInteger();
        }
        String terminalSchema = new Scanner(Transactions.class.getResourceAsStream("/generators/terminal.json"), "UTF-8").useDelimiter("\\A").next();

        ZipSampler zipSampler = new ZipSampler();
        zipSampler.setUseAddress("true");
        try {
            transactionGenerator.set(new SchemaSampler(terminalSchema));
        } catch (IOException e) {
            throw new InstantiationException(e.getMessage());
        }
        ObjectMapper objectMapper = new ObjectMapper();
        IntStream.range(0,terminals).forEach( x -> {
            JsonNode node = transactionGenerator.get().sample();
            final String terminal_id = node.get("terminal_id").asText();
            org.codehaus.jackson.JsonNode convertedNode = null;
            try {

                convertedNode = objectMapper.readTree( node.toString());
                ObjectNode newNode = new ObjectNode(JsonNodeFactory.instance);
                newNode.put("terminal_id",terminal_id);
                JsonNode nn = zipSampler.sample();
                convertedNode = objectMapper.readTree( nn.get("address").toString());
                newNode.put("terminal_address",convertedNode);
                convertedNode = objectMapper.readTree( nn.get("city").toString());
                newNode.put("terminal_city",convertedNode);
                convertedNode = objectMapper.readTree( nn.get("state").toString());
                newNode.put("terminal_state",convertedNode);
                convertedNode = objectMapper.readTree( nn.get("zip").toString());
                newNode.put("terminal_zip",convertedNode);
                terminalMap.put(terminal_id,newNode);
            } catch (IOException e) {
            }

        });
        recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        maxTransactions = context.getProperty(MAX_TRANSACTIONS).asInteger();

        isFixed = context.getProperty(FIXED_TRANSACTIONS).asBoolean();
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties  = new ArrayList<>();
        properties.add(MAX_TRANSACTIONS);
        properties.add(FIXED_TRANSACTIONS);
        properties.add(TERMINALS_TO_GENERATE);
        properties.add(RECORD_WRITER);
        properties.add(RECORD_COUNT);
        properties.add(RECORD_READER_FACTORY);
        properties.add(GENERATE_INVALID_TRANSACTIONS);
        properties.add(ALLOW_ZERO_TRANSACTIONS);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = Collections.emptySet();
        return set;
    }

    public static org.codehaus.jackson.JsonNode merge(org.codehaus.jackson.JsonNode mainNode, org.codehaus.jackson.JsonNode updateNode) {

        Iterator<String> fieldNames = updateNode.getFieldNames();

        while (fieldNames.hasNext()) {
            String updatedFieldName = fieldNames.next();
            org.codehaus.jackson.JsonNode valueToBeUpdated = mainNode.get(updatedFieldName);
            org.codehaus.jackson.JsonNode updatedValue = updateNode.get(updatedFieldName);

            // If the node is an @ArrayNode
            if (valueToBeUpdated != null && valueToBeUpdated.isArray() &&
                    updatedValue.isArray()) {
                // running a loop for all elements of the updated ArrayNode
                for (int i = 0; i < updatedValue.size(); i++) {
                    org.codehaus.jackson.JsonNode updatedChildNode = updatedValue.get(i);
                    // Create a new Node in the node that should be updated, if there was no corresponding node in it
                    // Use-case - where the updateNode will have a new element in its Array
                    if (valueToBeUpdated.size() <= i) {
                        ((ArrayNode) valueToBeUpdated).add(updatedChildNode);
                    }
                    // getting reference for the node to be updated
                    org.codehaus.jackson.JsonNode childNodeToBeUpdated = valueToBeUpdated.get(i);
                    merge(childNodeToBeUpdated, updatedChildNode);
                }
                // if the Node is an @ObjectNode
            } else if (valueToBeUpdated != null && valueToBeUpdated.isObject()) {
                merge(valueToBeUpdated, updatedValue);
            } else {
                if (mainNode instanceof ObjectNode) {
                    //((ObjectNode) mainNode).remove(updatedFieldName);
                    ((ObjectNode) mainNode).put(updatedFieldName, updatedValue);
                }
            }
        }
        return mainNode;
    }

    private void createRecord(){

    }

    protected ObjectNode createTerminalAndTransactions(final SchemaSampler mySampler, final List<String> keysAsArray,final int transactionCount) throws IOException {

        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("transactions", new ArrayNode(JsonNodeFactory.instance));
        ObjectMapper objectMapper = new ObjectMapper();
        for(int i=0; i < transactionCount; i++) {

            com.fasterxml.jackson.databind.JsonNode newNode = mySampler.sample();
            org.codehaus.jackson.JsonNode convertedNode = objectMapper.readTree(newNode.toString());
            org.codehaus.jackson.JsonNode terminal = terminalMap.get(keysAsArray.get(rand.nextInt(keysAsArray.size())));
            merge(convertedNode, terminal);
            ((ArrayNode) node.get("transactions")).add(convertedNode);
        }
        return node;
    }

    protected void createRecords(final ProcessSession processSession, final ProcessContext processContext, final int recordCount, final int maxTransactions){
        final RecordSetWriterFactory writerFactory = processContext.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        boolean invalid = processContext.getProperty(GENERATE_INVALID_TRANSACTIONS).asBoolean();
        final String definedSchema = invalid ? new Scanner(Transactions.class.getResourceAsStream("/generators/transaction.json"), "UTF-8").useDelimiter("\\A").next() :
                new Scanner(Transactions.class.getResourceAsStream("/generators/valid_transaction.json"), "UTF-8").useDelimiter("\\A").next();

        final SchemaSampler mySampler;
        try{
            mySampler = new SchemaSampler(definedSchema);
        }catch(final IOException e){
            throw new ProcessException(e);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        List<String> keysAsArray = new ArrayList<>(terminalMap.keySet());
        final RecordSchema schema;
        {


            try {
                final ObjectNode created = createTerminalAndTransactions(mySampler,keysAsArray,maxTransactions);
                String data = created.toString();
                Queue<org.codehaus.jackson.JsonNode> nodes = new ArrayDeque<>();

                org.codehaus.jackson.JsonNode node = objectMapper.readTree(data);
                nodes.add(node);
                final SchemaInferenceEngine<org.codehaus.jackson.JsonNode> timestampInference = new JsonSchemaInference(new TimeValueInference("yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
                RecordSource<org.codehaus.jackson.JsonNode> rs = new RecordSource<org.codehaus.jackson.JsonNode>() {

                    @Override
                    public org.codehaus.jackson.JsonNode next() throws IOException {

                        if (!nodes.isEmpty())
                        {
                            return nodes.poll();
                        }
                        else{
                            return null;
                        }
                    }
                };

                schema = timestampInference.inferSchema(rs);
            } catch (IOException e) {
                throw new ProcessException(e);
            }
        }



        final FlowFile flowFile = processSession.write(processSession.create(), (inputStream, out) -> {
            Map<String, String> obj = new HashMap<>();
            try {
                final RecordSchema writeSchema = writerFactory.getSchema(obj, schema);
                try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out)) {

                    IntStream.range(0, recordCount).forEach(x -> {

                        try {
                            ObjectNode node = createTerminalAndTransactions(mySampler,keysAsArray,maxTransactions);

                            final InputStream targetStream = IOUtils.toInputStream(node.toString(), StandardCharsets.UTF_8.name());
                            JsonTreeRowRecordReader rreader = new JsonTreeRowRecordReader(targetStream, getLogger(), schema, "yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                            Record record = rreader.nextRecord();

                            writer.write(record);

                        } catch (IOException e) {
                            throw new ProcessException(e);
                        } catch (MalformedRecordException e) {
                            throw new ProcessException(e);
                        }

                    });

                }
            }catch (IOException e) {
                throw new ProcessException(e);
            } catch (SchemaNotFoundException e) {
                throw new ProcessException(e);
            }


        });

        processSession.transfer(flowFile, REL_SUCCESS);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        FlowFile flowFile = processSession.get();
        if (null == flowFile) {
            int recordCount = processContext.getProperty(RECORD_COUNT).asInteger();
            int max =(maxTransactions > 1 ? rand.nextInt(5) : isFixed ? maxTransactions : rand.nextInt(maxTransactions));
            if (0 == max && processContext.getProperty(ALLOW_ZERO_TRANSACTIONS).asBoolean()){
                max = 1;
            }
            createRecords(processSession,processContext,recordCount,max);
            return;
        }

        Queue<org.codehaus.jackson.JsonNode> nodes = new ArrayDeque<>();
        final SchemaInferenceEngine<org.codehaus.jackson.JsonNode> timestampInference = new JsonSchemaInference(new TimeValueInference("yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
        final RecordSource<org.codehaus.jackson.JsonNode> rs = new RecordSource<org.codehaus.jackson.JsonNode>() {

            @Override
            public org.codehaus.jackson.JsonNode next() throws IOException {

                if (!nodes.isEmpty())
                {
                    return nodes.poll();
                }
                else{
                    return null;
                }
            }
        };


        final RecordSetWriterFactory writerFactory = processContext.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        boolean invalid = processContext.getProperty(GENERATE_INVALID_TRANSACTIONS).asBoolean();
        final String definedSchema = invalid ? new Scanner(Transactions.class.getResourceAsStream("/generators/transaction.json"), "UTF-8").useDelimiter("\\A").next() :
                new Scanner(Transactions.class.getResourceAsStream("/generators/valid_transaction.json"), "UTF-8").useDelimiter("\\A").next();

        final SchemaSampler mySampler;
        try{
            mySampler = new SchemaSampler(definedSchema);
        }catch(final IOException e){
            throw new ProcessException(e);
        }

        AtomicReference<Relationship> rel = new AtomicReference<>(REL_SUCCESS);

        final AtomicReference<RecordSchema> writeSchema = new AtomicReference<>(null);
        final AtomicReference<RecordSetWriter> recordWriter = new AtomicReference<>(null);
        final FlowFile newFlowFile = processSession.write(processSession.create(), (inputStream, out) -> {
            Map<String, String> obj = new HashMap<>();
            try {

                List<String> keysAsArray = new ArrayList<>(terminalMap.keySet());
                try (final InputStream in = processSession.read(flowFile);
                     final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {

                    Record record = reader.nextRecord();

                    do{
                        String tree = "{ \"object\": \"value\"}";
                        try {
                            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            final DataOutputStream dos = new DataOutputStream(bos);

                            RecordSetWriter rsw = JsonWriter.createWriter(getLogger(), reader.getSchema(), dos, obj);
                            rsw.write(record);
                            rsw.close();
                            tree = bos.toString();
                        }
                        catch(Exception e){
                            rel.set(REL_FAILURE);
                        }

                        ObjectMapper objectMapper = new ObjectMapper();
                        ObjectNode node = ObjectNode.class.cast( objectMapper.readTree(tree ));
                        node.put("transactions", new ArrayNode(JsonNodeFactory.instance));
                        int max =(maxTransactions > 1 ? rand.nextInt(5) : isFixed ? maxTransactions : rand.nextInt(maxTransactions));
                        if (0 == max && processContext.getProperty(ALLOW_ZERO_TRANSACTIONS).asBoolean()){
                            max = 1;
                        }
                        for(int i=0; i < max; i++){



                            com.fasterxml.jackson.databind.JsonNode newNode = mySampler.sample();
                            org.codehaus.jackson.JsonNode convertedNode = objectMapper.readTree( newNode.toString());
                            org.codehaus.jackson.JsonNode terminal = terminalMap.get(keysAsArray.get(rand.nextInt(keysAsArray.size())));
                            merge(convertedNode,terminal);
                            ((ArrayNode)node.get("transactions")).add(convertedNode);
                        }
                        nodes.add(node);


                            RecordSchema schema = timestampInference.inferSchema(rs);

                            InputStream targetStream = IOUtils.toInputStream(node.toString(), StandardCharsets.UTF_8.name());
                            JsonTreeRowRecordReader rreader = new JsonTreeRowRecordReader(targetStream, getLogger(), schema, "yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                            Record newRecord = rreader.nextRecord();

                            if (null == writeSchema.get()){
                                writeSchema.set(writerFactory.getSchema(obj, schema));
                                recordWriter.set(writerFactory.createWriter(getLogger(), writeSchema.get(), out));
                            }
                            recordWriter.get().write(newRecord);

                    }while ((record = reader.nextRecord()) != null);

                } catch (MalformedRecordException e) {
                    throw new ProcessException(e);
                }

            }catch (IOException e) {
                throw new ProcessException(e);
            } catch (SchemaNotFoundException e) {
                throw new ProcessException(e);
            }
            finally{
                if (null != recordWriter.get()){
                    recordWriter.get().close();
                }
            }
        });
        processSession.transfer(flowFile,REL_ORIGINAL);
        processSession.transfer(newFlowFile, rel.get());



    }
}
