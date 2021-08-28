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
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.SamplerBase;
import org.apache.nifi.datasynthesizer.services.LookupSampler;
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
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.nifi.datasynthesizer.services.SchemaLookupService;

import javax.xml.validation.Schema;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.IntStream;


@Tags({"data-synthesis", "record"})
public class DataSynthesizerBase extends AbstractProcessor {

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("schema")
            .displayName("Record Schema")
            .description("If defined, this schema will be used. If a schema lookup service is used this will function as the name to lookup against")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor SCHEMA_LOOKUP_SERVICE = new PropertyDescriptor.Builder()
            .name("schema-lookup")
            .displayName("Schema Lookup Service")
            .description("Specifies the Controller in which we will look-up service")
            .identifiesControllerService(SchemaLookupService.class)
            .required(false)
            .build();


    public static final PropertyDescriptor RECORD_COUNT = new PropertyDescriptor.Builder()
            .name("record-count")
            .displayName("Record Count")
            .description("Number of records to create per iteration")
            .defaultValue("1000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    protected SchemaLookupService schemaLookupService = null;


    public DataSynthesizerBase(){

    }


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

        return set;
    }

    protected String definedSchema = null;

    protected ThreadLocal<SamplerBase> sampler = new ThreadLocal<>();

    protected void loadSchema() throws IOException {
        if (null != schemaLookupService)
        {
            Optional<JsonNode> result = schemaLookupService.sampleSchema(definedSchema);
            if (result.isPresent()){
                sampler.set(new LookupSampler(schemaLookupService,definedSchema));
            }
        }
        if (sampler.get() == null) {
            try {
                sampler.set(new SchemaSampler(definedSchema));
            } catch (MismatchedInputException mie) {
                sampler.set(new SchemaSampler("[" + definedSchema + "]"));
            }
        }
    }

    protected void createRecords(final ProcessSession processSession, final ProcessContext processContext, final int recordCount){

        final SamplerBase mySampler;
        try {
            if (definedSchema != null) {
                if (sampler.get() == null) {
                    loadSchema();
                }
                mySampler = sampler.get();
            } else {

                FlowFile ff = processSession.get();
                if (ff == null) {
                    return;
                }
                try (final InputStream in = processSession.read(ff)) {
                    final String schema = IOUtils.toString(in, StandardCharsets.UTF_8.name());
                    mySampler = new SchemaSampler(definedSchema);
                }
                processSession.remove(ff);
            }
        } catch (IOException e) {
            throw new ProcessException("Could not create schema",e);
        }
        final RecordSetWriterFactory writerFactory = processContext.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);


        final RecordSchema schema;
        {
            final JsonNode created = mySampler.nextSample().orElseThrow();

            try {
                String data = created.toString();
                ObjectMapper objectMapper = new ObjectMapper();
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


                        JsonNode created = mySampler.nextSample().orElseThrow();
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

        processSession.transfer(flowFile, REL_SUCCESS);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        int recordCount = processContext.getProperty(RECORD_COUNT).asInteger();
        createRecords(processSession,processContext,recordCount);
    }
}