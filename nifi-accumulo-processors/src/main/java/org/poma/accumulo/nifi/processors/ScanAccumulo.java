/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.poma.accumulo.nifi.processors;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;
import org.poma.accumulo.nifi.controllerservices.BaseAccumuloService;
import org.poma.accumulo.nifi.data.KeySchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "accumulo", "scan", "record"})
/**
 * Purpose and Design: Requires a connector be defined by way of an AccumuloService object. This class
 * simply extends BaseAccumuloProcessor to scan accumulo based on aspects and expression executed against
 * a flow file
 *
 */
public class ScanAccumulo extends BaseAccumuloProcessor {



    static final PropertyDescriptor START_KEY = new PropertyDescriptor.Builder()
            .displayName("Start key")
            .name("start-key")
            .description("Start row key")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor START_KEY_INCLUSIVE = new PropertyDescriptor.Builder()
            .displayName("Start key Inclusive")
            .name("start-key-inclusive")
            .description("Determines if the start key is inclusive ")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor END_KEY = new PropertyDescriptor.Builder()
            .displayName("End key")
            .name("end-key")
            .description("End row key for this. If not specified or empty this will be infinite")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor END_KEY_INCLUSIVE = new PropertyDescriptor.Builder()
            .displayName("End key Inclusive")
            .name("end-key-inclusive")
            .description("Determines if the end key is inclusive")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor AUTHORIZATIONS = new PropertyDescriptor.Builder()
            .name("accumulo-authorizations")
            .displayName("Authorizations")
            .description("The comma separated list of authorizations to pass to the scanner.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully retrieved from Accumulo")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be retrieved fromAccumulo")
            .build();

    static final PropertyDescriptor RECORD_WRITER = (new PropertyDescriptor.Builder()).name("record-writer").displayName("Record Writer").description("Specifies the Controller Service to use for writing out the records").identifiesControllerService(RecordSetWriterFactory.class).required(true).build();

    /**
     * Connector service which provides us a connector if the configuration is correct.
     */
    protected BaseAccumuloService accumuloConnectorService;

    /**
     * Connector that we need to persist while we are operational.
     */
    protected Connector connector;


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        accumuloConnectorService = context.getProperty(ACCUMULO_CONNECTOR_SERVICE).asControllerService(BaseAccumuloService.class);
        this.connector = accumuloConnectorService.getConnector();

    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final RecordSetWriterFactory writerFactory = processContext.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final LongAdder recordCounter= new LongAdder();


        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = flowFile.getAttributes();
        final String table = processContext.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String startKey = processContext.getProperty(START_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String endKey = processContext.getProperty(END_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String authorizations = processContext.getProperty(AUTHORIZATIONS).evaluateAttributeExpressions(flowFile).getValue();
        final int threads = processContext.getProperty(THREADS).asInteger();

        final Authorizations auths;
        if (!StringUtils.isBlank(authorizations))
             auths = new Authorizations(authorizations.split(","));
        else
            auths = new Authorizations();

        Range lookupRange = new Range(StringUtils.isEmpty(startKey) ? null : startKey,StringUtils.isEmpty(endKey) ? null : endKey);

        try (BatchScanner scanner = connector.createBatchScanner(table,auths,threads)) {
            scanner.setRanges(Collections.singleton(lookupRange));

            final Iterator<Map.Entry<Key,Value>> kvIter = scanner.iterator();
            if (!kvIter.hasNext()){
                final Map<String, String> attributes = new HashMap<>();
                    attributes.put("record.count", String.valueOf(0));
                    processSession.putAllAttributes(flowFile,attributes);
                    processSession.transfer(flowFile, REL_SUCCESS);
                return;
            } else{

                while (kvIter.hasNext()) {
                    FlowFile clonedFlowFile = processSession.clone(original);

                    final int keysPerFlowFile = 1000;
                    final Map<String, String> attributes = new HashMap<>();
                    clonedFlowFile = processSession.write(clonedFlowFile, new StreamCallback() {
                        @Override
                        public void process(final InputStream in, final OutputStream out) throws IOException {

                            try{
                            final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, new KeySchema());
                            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out)) {

                                int i = 0;
                                for (; i < keysPerFlowFile && kvIter.hasNext(); i++) {

                                    Map.Entry<Key, Value> kv = kvIter.next();

                                    Key key = kv.getKey();

                                    writer.beginRecordSet();

                                    Map<String, Object> data = new HashMap<>();
                                    data.put("row", key.getRow().toString());
                                    data.put("columnFamily", key.getColumnFamily().toString());
                                    data.put("columnQualifier", key.getColumnQualifier().toString());
                                    data.put("columnVisibility", key.getColumnVisibility().toString());
                                    data.put("timestamp", key.getTimestamp());

                                    MapRecord record = new MapRecord(new KeySchema(), data);
                                    writer.write(record);


                                }
                                recordCounter.add(i);

                                final WriteResult writeResult = writer.finishRecordSet();
                                attributes.put("record.count", String.valueOf(i));
                                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                                attributes.putAll(writeResult.getAttributes());
                            }
                            } catch (SchemaNotFoundException e) {
                                getLogger().error("Failed to process {}; will route to failure", new Object[] {flowFile, e});
                                throw new IOException(e);
                            }
                        }

                    });
                    processSession.putAllAttributes(clonedFlowFile,attributes);
                    processSession.transfer(clonedFlowFile, REL_SUCCESS);
                }
            }
        } catch (final Exception e) {
            e.printStackTrace();
            getLogger().error("Failed to process {}; will route to failure", new Object[] {flowFile, e});
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }

        processSession.remove(flowFile);

        processSession.adjustCounter("Records Processed", recordCounter.longValue(), false);
        getLogger().info("Successfully converted {} records for {}", new Object[] {recordCounter.longValue(), flowFile});
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(START_KEY);
        properties.add(START_KEY_INCLUSIVE);
        properties.add(END_KEY);
        properties.add(END_KEY_INCLUSIVE);
        properties.add(RECORD_WRITER);
        properties.add(AUTHORIZATIONS);
        return properties;
    }

}
