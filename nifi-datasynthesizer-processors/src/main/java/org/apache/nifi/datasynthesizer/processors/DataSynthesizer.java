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
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.SamplerBase;
import org.apache.nifi.datasynthesizer.services.SchemaLookupService;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.IntStream;


@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"data-synthesis", "record"})
public class DataSynthesizer extends DataSynthesizerBase {

    public DataSynthesizer(){

    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA);
        properties.add(RECORD_COUNT);
        properties.add(RECORD_WRITER);
        properties.add(SCHEMA_LOOKUP_SERVICE);
        return properties;
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = Collections.emptySet();
        return set;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        sampler = new ThreadLocal<>();
        if (context.getProperty(SCHEMA).isSet()) {
            definedSchema = context.getProperty(SCHEMA).getValue();
        }
        if (context.getProperty(SCHEMA_LOOKUP_SERVICE).isSet()) {
            schemaLookupService = context.getProperty(SCHEMA_LOOKUP_SERVICE).asControllerService(SchemaLookupService.class);
        }
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        int recordCount = processContext.getProperty(RECORD_COUNT).asInteger();
        createRecords(processSession,processContext,recordCount);
    }
/*
    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        final SamplerBase mySampler;
        try {
            if (definedSchema != null) {
                if (sampler.get() == null) {
                    try {
                        sampler.set(new SchemaSampler(definedSchema));
                    }catch( MismatchedInputException mie){
                        sampler.set(new SchemaSampler("[" + definedSchema + "]"));
                    }
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
            e.printStackTrace();
            throw new ProcessException("Could not create schema",e);
        }
        final RecordSetWriterFactory writerFactory = processContext.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);


        int recordCount = processContext.getProperty(RECORD_COUNT).asInteger();
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



    }*/
}