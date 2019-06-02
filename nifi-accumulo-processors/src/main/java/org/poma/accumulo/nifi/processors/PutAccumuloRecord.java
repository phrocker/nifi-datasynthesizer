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


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.apache.nifi.util.StringUtils;
import org.poma.accumulo.nifi.controllerservices.AccumuloService;
import org.poma.accumulo.nifi.data.AccumuloRecordConfiguration;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "accumulo", "put", "record"})
@DynamicProperties({
        @DynamicProperty(name = "visibility.<COLUMN FAMILY>", description = "Visibility label for everything under that column family " +
                "when a specific label for a particular column qualifier is not available.", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                value = "visibility label for <COLUMN FAMILY>"
        ),
        @DynamicProperty(name = "visibility.<COLUMN FAMILY>.<COLUMN QUALIFIER>", description = "Visibility label for the specified column qualifier " +
                "qualified by a configured column family.", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                value = "visibility label for <COLUMN FAMILY>:<COLUMN QUALIFIER>."
        )
})
public class PutAccumuloRecord extends AbstractProcessor {


    protected static final PropertyDescriptor ACCUMULO_CONNECTOR_SERVICE = new PropertyDescriptor.Builder()
            .name("Accumulo Connector Service")
            .description("Specifies the Controller Service to use for accessing Accumulo.")
            .required(true)
            .identifiesControllerService(AccumuloService.class)
            .build();

    protected static final PropertyDescriptor MEMORY_SIZE = new PropertyDescriptor.Builder()
            .name("Memory Size")
            .description("The maximum memory size Accumulo at any one time from the record set.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("10 MB")
            .build();

    protected static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder()
            .name("Column Family")
            .description("The Column Family to use when inserting data into Accumulo")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor RECORD_IN_QUALIFIER = new PropertyDescriptor.Builder()
            .name("record-value-in-qualifier")
            .displayName("Record Value In Qualifier")
            .description("Places the record value into the column qualifier instead of the value.")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    protected static final PropertyDescriptor ROW_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("Row Identifier Field Name")
            .description("Specifies the name of a record field whose value should be used as the row id for the given record.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Accumulo Table into which data will be placed")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("timestamp-field")
            .displayName("Timestamp Field")
            .description("Specifies the name of a record field whose value should be used as the timestamp. If empty a timestamp will be recorded as the time of insertion")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor WRITE_THREADS = new PropertyDescriptor.Builder()
            .name("Write Threads")
            .description("Number of write threads allowed for a batch writer")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    protected static final PropertyDescriptor VISIBILITY_PATH = new PropertyDescriptor.Builder()
            .name("visibility-path")
            .displayName("Visibility String Record Path Root")
            .description("A record path that points to part of the record which contains a path to a mapping of visibility strings to record paths")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    protected static final PropertyDescriptor DEFAULT_VISIBILITY = new PropertyDescriptor.Builder()
            .name("default-visibility")
            .displayName("Default Visibility")
            .description("Default visibility when VISIBILITY_PATH is not defined. ")
            .required(false)
            .addValidator(Validator.VALID)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in Accumulo")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Accumulo")
            .build();


    // service
    protected AccumuloService accumuloConnectorService;

    protected Connector connector;

    private MultiTableBatchWriter tableWriter = null;

    /**
     * Record path cache
     */
    protected RecordPathCache recordPathCache;

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        accumuloConnectorService = context.getProperty(ACCUMULO_CONNECTOR_SERVICE).asControllerService(AccumuloService.class);
        final Double maxBytes = context.getProperty(MEMORY_SIZE).asDataSize(DataUnit.B);
        this.connector = accumuloConnectorService.getConnector();
        BatchWriterConfig writerConfig = new BatchWriterConfig();
        writerConfig.setMaxWriteThreads(context.getProperty(WRITE_THREADS).asInteger());
        writerConfig.setMaxMemory(maxBytes.longValue());
        tableWriter = connector.createMultiTableBatchWriter(writerConfig);
    }

    @OnDisabled
    public void shutdown(){
        if (null != tableWriter){
            try {
                tableWriter.close();
            } catch (MutationsRejectedException e) {
                getLogger().error("Mutations were rejected",e);
            }
        }
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER_FACTORY);
        properties.add(ACCUMULO_CONNECTOR_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(ROW_FIELD_NAME);
        properties.add(ROW_FIELD_NAME);
        properties.add(COLUMN_FAMILY);
        properties.add(WRITE_THREADS);
        properties.add(VISIBILITY_PATH);
        properties.add(DEFAULT_VISIBILITY);
        properties.add(MEMORY_SIZE);
        properties.add(RECORD_IN_QUALIFIER);
        properties.add(TIMESTAMP_FIELD);
        return properties;
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        /**
         * Adapted from HBase puts. This is a good approach and one that we should adopt here, too.
         */
        if (propertyDescriptorName.startsWith("visibility.")) {
            String[] parts = propertyDescriptorName.split("\\.");
            String displayName;
            String description;

            if (parts.length == 2) {
                displayName = String.format("Column Family %s Default Visibility", parts[1]);
                description = String.format("Default visibility setting for %s", parts[1]);
            } else if (parts.length == 3) {
                displayName = String.format("Column Qualifier %s.%s Default Visibility", parts[1], parts[2]);
                description = String.format("Default visibility setting for %s.%s", parts[1], parts[2]);
            } else {
                return null;
            }

            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(displayName)
                    .description(description)
                    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .dynamic(true)
                    .build();
        }

        return null;
    }


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory recordParserFactory = processContext.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        final String recordPathText = processContext.getProperty(VISIBILITY_PATH).getValue();
        final String defaultVisibility = processContext.getProperty(DEFAULT_VISIBILITY).isSet() ? processContext.getProperty(DEFAULT_VISIBILITY).getValue() : null;

        AccumuloRecordConfiguration builder = AccumuloRecordConfiguration.Builder.newBuilder().
                setTableName(processContext.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue()).
                setColumnFamily(processContext.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue()).
                setRowField(processContext.getProperty(ROW_FIELD_NAME).evaluateAttributeExpressions(flowFile).getValue()).
                setQualifierInKey(processContext.getProperty(RECORD_IN_QUALIFIER).isSet() ? processContext.getProperty(RECORD_IN_QUALIFIER).asBoolean() : false).
                setTimestampField(processContext.getProperty(TIMESTAMP_FIELD).evaluateAttributeExpressions(flowFile).getValue()).build();


        RecordPath recordPath = null;
        if (recordPathCache != null && !StringUtils.isEmpty(recordPathText)) {
            recordPath = recordPathCache.getCompiled(recordPathText);
        }

        final long start = System.nanoTime();
        int index = 0;
        int columns = 0;
        boolean failed = false;
        Mutation prevMutation=null;
        int count=0;
        try (final InputStream in = processSession.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
            Record record;

            while ((record = reader.nextRecord()) != null) {

                prevMutation = createMutation(prevMutation, processContext, record, reader.getSchema(), recordPath, flowFile,defaultVisibility,  builder);
                count++;
                index++;

            }
            addMutation(builder.getTableName(),prevMutation);
        } catch (Exception ex) {
            getLogger().error("Failed to put records to Accumulo.", ex);
            failed = true;
        }



        if (!failed) {
            processSession.transfer(flowFile, REL_SUCCESS);
        } else {
            flowFile = processSession.penalize(flowFile);
            processSession.transfer(flowFile, REL_FAILURE);
        }

        try {
            tableWriter.flush();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }

        processSession.commit();
    }

    /**
     * Adapted from HBASEUtils. Their approach seemed ideal for what our intent is here.
     * @param columnFamily
     * @param columnQualifier
     * @param flowFile
     * @param context
     * @return
     */
    public static String produceVisibility(String columnFamily, String columnQualifier, FlowFile flowFile, ProcessContext context) {
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(columnFamily)) {
            return null;
        }
        String lookupKey = String.format("visibility.%s%s%s", columnFamily, !org.apache.commons.lang3.StringUtils.isNotEmpty(columnQualifier) ? "." : "", columnQualifier);
        String fromAttribute = flowFile.getAttribute(lookupKey);

        if (fromAttribute == null && !org.apache.commons.lang3.StringUtils.isBlank(columnQualifier)) {
            String lookupKeyFam = String.format("visibility.%s", columnFamily);
            fromAttribute = flowFile.getAttribute(lookupKeyFam);
        }

        if (fromAttribute != null) {
            return fromAttribute;
        } else {
            PropertyValue descriptor = context.getProperty(lookupKey);
            if (descriptor == null || !descriptor.isSet()) {
                descriptor = context.getProperty(String.format("visibility.%s", columnFamily));
            }

            String retVal = descriptor != null ? descriptor.evaluateAttributeExpressions(flowFile).getValue() : null;

            return retVal;
        }
    }

    private void addMutation(final String tableName, final Mutation m) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        tableWriter.getBatchWriter(tableName).addMutation(m);

    }


    /**
     * Creates a mutation with the provided arguments
     * @param prevMutation previous mutation, to append to if in the same row.
     * @param context process context.
     * @param record record object extracted from the flow file
     * @param schema schema for this record
     * @param recordPath record path for visibility extraction
     * @param flowFile flow file
     * @param defaultVisibility default visibility
     * @param config configuration of this instance.
     * @return
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected Mutation createMutation(final Mutation prevMutation,
                                      final ProcessContext context,
                                      final Record record,
                                      final RecordSchema schema,
                                      final RecordPath recordPath,
                                      final FlowFile flowFile,
                                      final String defaultVisibility,
                                      AccumuloRecordConfiguration config) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Mutation m=null;
        if (record != null) {

            final Long timestamp;
            Set<String> fieldsToSkip = new HashSet<>();
            if (!StringUtils.isBlank(config.getTimestampField())) {
                try {
                    timestamp = record.getAsLong(config.getTimestampField());
                    fieldsToSkip.add(config.getTimestampField());
                } catch (IllegalTypeConversionException e) {
                    throw new AccumuloException("Could not convert " + config.getTimestampField() + " to a long", e);
                }

                if (timestamp == null) {
                    getLogger().warn("The value of timestamp field " + config.getTimestampField() + " was null, record will be inserted with latest timestamp");
                }
            } else {
                timestamp = null;
            }



            RecordField visField = null;
            Map visSettings = null;
            if (recordPath != null) {
                final RecordPathResult result = recordPath.evaluate(record);
                FieldValue fv = result.getSelectedFields().findFirst().get();
                visField = fv.getField();
                if (null != visField)
                fieldsToSkip.add(visField.getFieldName());
                visSettings = (Map)fv.getValue();
            }


            if (null != prevMutation){
                Text row = new Text(prevMutation.getRow());
                Text curRow = new Text(record.getAsString(config.getRowFIeld()));
                if (row.equals(curRow)){
                    m = prevMutation;
                }
                else{
                    m = new Mutation(curRow);
                    addMutation(config.getTableName(),prevMutation);
                }
            }
            else{
                Text row = new Text(record.getAsString(config.getRowFIeld()));
                m = new Mutation(row);
            }

            fieldsToSkip.add(config.getRowFIeld());

            Text cf = new Text(config.getColumnFamily());


            for (String name : schema.getFieldNames().stream().filter(p->!fieldsToSkip.contains(p)).collect(Collectors.toList())) {


                String visString = (visField != null && visSettings != null && visSettings.containsKey(name))
                        ? (String)visSettings.get(name) : defaultVisibility;

                Text cq = new Text(name);
                final Value value;
                String recordValue  = record.getAsString(name);
                if (config.getQualifierInKey()){
                    cq.append(recordValue.getBytes(),0,recordValue.length());
                    value = new Value();
                }
                else{
                    value = new Value(recordValue.getBytes());
                }

                if (StringUtils.isBlank(visString)) {
                    visString = produceVisibility(cf.toString(), cq.toString(), flowFile, context);
                }

                ColumnVisibility cv = new ColumnVisibility();
                if (StringUtils.isBlank(visString)) {
                    if (!StringUtils.isBlank(defaultVisibility)) {
                        cv = new ColumnVisibility(defaultVisibility);
                    }
                }
                else
                {
                    cv = new ColumnVisibility(visString);
                }

                if (null != timestamp) {
                    m.put(cf, cq, cv, timestamp, value);
                }
                else{
                    m.put(cf, cq, cv, value);
                }
            }



        }

        return m;
    }
}