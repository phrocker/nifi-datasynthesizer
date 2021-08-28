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

import com.mapr.synth.samplers.SchemaSampler;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.data.JsonWriter;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
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
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"data-synthesis", "record"})
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
            .required(true)
            .build();

    protected    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
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
        properties.add(RECORD_READER_FACTORY);
        properties.add(SELECT_OBJECT);
        return properties;
    }


    protected RecordReaderFactory recordParserFactory;


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Data could be synthesized")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original data record.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Data could not be synthesized")
            .build();

    public static final Relationship REL_CORRELATION = new Relationship.Builder()
            .name("correlation")
            .description("Correlation relationship")
            .build();


            static final AllowableValue ALWAYS_SUPPRESS = new AllowableValue("always-suppress", "Always Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null, will not be written out");
    static final AllowableValue NEVER_SUPPRESS = new AllowableValue("never-suppress", "Never Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null, will be written out as a null value");
    static final AllowableValue SUPPRESS_MISSING = new AllowableValue("suppress-missing", "Suppress Missing Values",
            "When a field has a value of null, it will be written out. However, if a field is defined in the schema and not present in the record, the field will not be written out.");

    static final AllowableValue OUTPUT_ARRAY = new AllowableValue("output-array", "Array",
            "Output records as a JSON array");
    static final AllowableValue OUTPUT_ONELINE = new AllowableValue("output-oneline", "One Line Per Object",
            "Output records with one JSON object per line, delimited by a newline character");

    public static final String COMPRESSION_FORMAT_GZIP = "gzip";
    public static final String COMPRESSION_FORMAT_BZIP2 = "bzip2";
    public static final String COMPRESSION_FORMAT_XZ_LZMA2 = "xz-lzma2";
    public static final String COMPRESSION_FORMAT_SNAPPY = "snappy";
    public static final String COMPRESSION_FORMAT_SNAPPY_FRAMED = "snappy framed";
    public static final String COMPRESSION_FORMAT_NONE = "none";

    static final PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
            .name("suppress-nulls")
            .displayName("Suppress Null Values")
            .description("Specifies how the writer should handle a null field")
            .allowableValues(NEVER_SUPPRESS, ALWAYS_SUPPRESS, SUPPRESS_MISSING)
            .defaultValue(NEVER_SUPPRESS.getValue())
            .required(true)
            .build();
    static final PropertyDescriptor PRETTY_PRINT_JSON = new PropertyDescriptor.Builder()
            .name("Pretty Print JSON")
            .description("Specifies whether or not the JSON should be pretty printed")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    static final PropertyDescriptor SELECT_OBJECT = new PropertyDescriptor.Builder()
            .name("select-object")
            .displayName("Select object")
            .description("If true an object with the key name will be selected from the resulting schema you produce. Otherwise the whole object will be placed into the key")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("true")
            .required(true)
            .build();
            
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        rels.add(REL_ORIGINAL);
        rels.add(REL_CORRELATION);
        return rels;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = Collections.emptySet();
        /*
        if (!validationContext.getProperty(SCHEMA).isSet() ||
            validationContext.getProperty(SCHEMA_KEY).isSet() ){
                set.add(new ValidationResult.Builder().explanation("Schema and Schema key must be defined").build());
    }*/
        return set;
    }

   //private JsonRecordSetWriter jsonRecordSetWriter = null;

    private boolean selectObject = true;

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);
        selectObject = context.getProperty(SELECT_OBJECT).asBoolean();
    }


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (null == flowFile)
            return;
        int recordCount = processContext.getProperty(RECORD_COUNT).asInteger(); 
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

        final String definedSchema = processContext.getProperty(SCHEMA).getValue();
        final String schema_key_name = processContext.getProperty(SCHEMA_KEY).getValue();

        /**
         * 
         */

        final SchemaSampler mySampler;
         try{
            mySampler = new SchemaSampler(definedSchema);
         }catch(final IOException e){
             throw new ProcessException(e);
         }
         /**
          * 

          */
          Random rand = new Random();
          final AtomicReference<RecordSchema> writeSchema = new AtomicReference<>(null);
          final AtomicReference<RecordSetWriter> recordWriter = new AtomicReference<>(null);
          final FlowFile newFlowFile = processSession.write(processSession.create(), (inputStream, out) -> {
            Map<String, String> obj = new HashMap<>();
            try {
                        
                    
                    //processContext.getProperty(SCHEMA).
                    try (final InputStream in = processSession.read(flowFile);
                        final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
                            
                            Record record = reader.nextRecord();

                            do{

                                for(int i=0; i < (recordCount > 1 ? rand.nextInt(5) : recordCount); i++){

                                    final ByteArrayOutputStream bos= new ByteArrayOutputStream();
                                    final DataOutputStream dos = new DataOutputStream(bos);
                                    
                                    RecordSetWriter rsw = JsonWriter.createWriter(getLogger(), reader.getSchema(), dos, obj);
                                    try {
                                        rsw.write(record);
                                    }
                                    catch(Exception e){
                                        continue;
                                    }
                                    rsw.close();

                                    ObjectMapper objectMapper = new ObjectMapper();
                                    ObjectNode node = (ObjectNode) objectMapper.readTree(bos.toString());
                                    
                                    com.fasterxml.jackson.databind.JsonNode newNode = mySampler.sample();
                                    if (selectObject)
                                    newNode = newNode.get(schema_key_name);
                                    JsonNode convertedNode = objectMapper.readTree( newNode.toString());

                                    node.put(schema_key_name,convertedNode);

                                    nodes.add(convertedNode);

                                    RecordSchema correlationSchema = timestampInference.inferSchema(rs);

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

                                    targetStream = IOUtils.toInputStream(convertedNode.toString(), StandardCharsets.UTF_8.name());
                                    rreader = new JsonTreeRowRecordReader(targetStream, getLogger(), correlationSchema, "yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                                    Record correlationRecord = rreader.nextRecord();
                        

                                    FlowFile correlationFlowFile = processSession.create();
                                    OutputStream outty = processSession.write(correlationFlowFile);
                                    try{
                                    RecordSetWriter wrt = writerFactory.createWriter(getLogger(), writerFactory.getSchema(obj, correlationSchema), outty);
                                    wrt.write(correlationRecord);
                                    wrt.close();
                                        } catch (SchemaNotFoundException e) {
                                            throw new ProcessException(e);
                                        }

                                    outty.close();

                                    processSession.transfer(correlationFlowFile, REL_CORRELATION);

                                }
                                //writer.write(newRecord);
                            }while ((record = reader.nextRecord()) != null);

                        } catch (MalformedRecordException e) {
                      // TODO Auto-generated catch block
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
        processSession.transfer(newFlowFile, REL_SUCCESS);



    }
}