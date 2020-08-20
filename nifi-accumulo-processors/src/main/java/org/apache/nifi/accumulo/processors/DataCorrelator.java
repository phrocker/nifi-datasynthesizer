package org.apache.nifi.accumulo.processors;

import com.mapr.synth.samplers.SchemaSampler;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonSchemaInference;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.IntStream;


@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"hadoop", "accumulo", "put", "record"})
public class DataCorrelator extends AbstractProcessor {

    protected static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("schema")
            .displayName("Record Schema")
            .description("If defined, this schema will be used. Otherwise flow input will be used")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    
    protected static final PropertyDescriptor SCHEMA_KEY = new PropertyDescriptor.Builder()
            .name("schema-key")
            .displayName("Record Schema Key")
            .description("Defines the key name that will be used for the resulting schema object")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    protected static final PropertyDescriptor RECORD_COUNT = new PropertyDescriptor.Builder()
            .name("record-count")
            .displayName("Record Count")
            .description("Number of records to create per iteration")
            .defaultValue("1000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public DataCorrelator(){

    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA);
        properties.add(SCHEMA_KEY);
        properties.add(RECORD_COUNT);
        properties.add(RECORD_WRITER);
        return properties;
    }


    private RecordReaderFactory recordParserFactory;


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Data could be synthesized")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Data could not be synthesized")
            .build();

    

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = Collections.emptySet();
        if (!validationContext.getProperty(SCHEMA).isSet() ||
            validationContext.getProperty(SCHEMA_KEY).isSet() ){
                set.add(new ValidationResult.Builder().explanation("Schema and Schema key must be defined").build());
    }
        return set;
    }

    


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException, TableExistsException, AccumuloSecurityException, AccumuloException {
        recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);
    }


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (null == flowFile)
            return;

        String definedSchema = null;
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

        if (processContext.getProperty(SCHEMA).isSet()) {
            definedSchema = processContext.getProperty(SCHEMA).getValue();
        }else{
            processContext.yield();
        }

        String schema_key_name = "";
        if (processContext.getProperty(SCHEMA_KEY).isSet()) {
            schema_key_name = processContext.getProperty(SCHEMA_KEY).getValue();
        }else{
            processContext.yield();
        }

        /**
         * 
         */

        final SchemaSampler mySampler = new SchemaSampler(definedSchema);
         /**
          * 

          */

          final FlowFile newFlowFile = processSession.write(processSession.create(), (inputStream, out) -> {
            Map<String, String> obj = new HashMap<>();
            try {
                
            
            //processContext.getProperty(SCHEMA).
            try (final InputStream in = processSession.read(flowFile);
                final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
                    
                    Record record = reader.nextRecord();
                    final ByteArrayOutputStream bos= new ByteArrayOutputStream();
                    final DataOutputStream dos = new DataOutputStream(bos);
                    JsonRecordSetWriter writer = new JsonRecordSetWriter();
                    RecordSetWriter rsw = writer.createWriter(getLogger(), reader.getSchema(), dos)
                    rsw.write(record);
                    rsw.close();

                    ObjectMapper objectMapper = new ObjectMapper();
                    ObjectNode node = ObjectNode.class.cast( objectMapper.readTree(bos.toString()) );
                    
                    com.fasterxml.jackson.databind.JsonNode newNode = mySampler.sample();
                    JsonNode convertedNode = objectMapper.readTree( newNode.textValue());

                    node.put(schema_key_name,convertedNode);

                    nodes.add((JsonNode)node);

                    RecordSchema schema = timestampInference.inferSchema(rs);

                    final InputStream targetStream = IOUtils.toInputStream(node.asText(), StandardCharsets.UTF_8.name());
                    JsonTreeRowRecordReader rreader = new JsonTreeRowRecordReader(targetStream, getLogger(), schema, "yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                    Record newRecord = rreader.nextRecord();

                }

            
                
            

                flowFile = processSession.write(processSession.create(), (inputStream, out) -> {
                Map<String, String> obj = new HashMap<>();
                try {
                    final RecordSchema writeSchema = writerFactory.getSchema(obj, schema);
                    try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out)) {

                    IntStream.range(0, recordCount).forEach(x -> {

    //                    JsonTreeReader reader = new JsonTreeReader();
    //                  Map<String, String> variables = new HashMap<>();


                        JsonNode created = mySampler.sample();
                        String data = created.toString();

                        try {
                            final InputStream targetStream = IOUtils.toInputStream(data, StandardCharsets.UTF_8.name());
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

        }catch (IOException e) {
                throw new ProcessException(e);
            } catch (SchemaNotFoundException e) {
                throw new ProcessException(e);
            }


        });

        processSession.transfer(flowFile, REL_SUCCESS);



    }
}