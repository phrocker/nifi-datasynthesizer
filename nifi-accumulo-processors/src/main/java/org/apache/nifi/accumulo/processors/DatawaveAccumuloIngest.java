package org.apache.nifi.accumulo.processors;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.config.DataTypeHelperImpl;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.nifi.accumulo.data.*;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


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
        ),
        @DynamicProperty(name = "FROM.<FIELD_NAME>", description = "Specifies the from edge relationship.", expressionLanguageScope = ExpressionLanguageScope.NONE,
                value = "Edge from relationships"
        ),
        @DynamicProperty(name = "TO.<FIELD_NAME>", description = "Specifies the to edge relationship.", expressionLanguageScope = ExpressionLanguageScope.NONE,
                value = "Edge to relationships"
        )
})
public abstract class DatawaveAccumuloIngest extends BaseAccumuloProcessor {
    protected static final PropertyDescriptor DATA_NAME = new PropertyDescriptor.Builder()
            .name("Data name")
            .description("Data type name of the data")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor INGEST_HELPER = new PropertyDescriptor.Builder()
            .name("Ingest Helper")
            .description("Ingest Helper class")
            .required(false)
            .defaultValue(RecordIngestHelper.class.getCanonicalName())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Hadoop Record reader class")
            .required(false)
            .defaultValue(DatawaveRecordReader.class.getCanonicalName())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DATA_HANDLER_CLASS = new PropertyDescriptor.Builder()
            .name("Handler Class")
            .description("Datawave handler class")
            .required(false)
            .defaultValue(ContentRecordHandler.class.getCanonicalName())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor FATAL_ERRORS = new PropertyDescriptor.Builder()
            .name("Fatal Errors")
            .description("Comma separated list of errors considered fatal")
            .required(false)
            .defaultValue("MISSING_DATA_ERROR,INVALID_DATA_ERROR")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor UUID_FIELDS = new PropertyDescriptor.Builder()
            .name("UUID Fields")
            .description("Comma separated list of fields used for UUID calculation")
            .required(false)
            .defaultValue("UUID,PARENT_UUID")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor INDEXED_FIELDS = new PropertyDescriptor.Builder()
            .name("Indexed Fields")
            .description("Comma separated list of fields used for UUID calculation")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor INDEX_ALL_FIELDS = new PropertyDescriptor.Builder()
            .name("Index All Fields")
            .description("True to index all fields")
            .required(false)
            .defaultValue("True").allowableValues("True","False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ENABLE_METADATA = new PropertyDescriptor.Builder()
            .name("Enable Metadata")
            .description("Enables datawave metadata table")
            .required(false)
            .defaultValue("True").allowableValues("True","False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ENABLE_METRICS = new PropertyDescriptor.Builder()
            .name("Enable Metrics")
            .description("Enables datawave ingest metrics")
            .required(false)
            .defaultValue("True").allowableValues("True","False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor METRICS_FIELDS = new PropertyDescriptor.Builder()
            .name("Metrics Fields")
            .description("Metrics Fields")
            .required(false)
            .defaultValue("table,fileExtension")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    protected static final PropertyDescriptor METRICS_RECEIVERS = new PropertyDescriptor.Builder()
            .name("MetricsReceivers")
            .description("Enables datawave ingest metrics")
            .required(false)
            .defaultValue(
                    Joiner.on(",").join(
                    Arrays.asList(EventCountReceiver.class.getCanonicalName(),
                    ByteCountReceiver.class.getCanonicalName(),
                    TableCountReceiver.class.getCanonicalName())))
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    protected static final PropertyDescriptor LABELS_CONFIG = new PropertyDescriptor.Builder()
            .name("Metrics Labels")
            .description("Metrics Labels")
            .required(false)
            .defaultValue("table=shard,table=shardIndex,dataType=*,handler=" + RecordDataTypeHelper.class.getName())
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ENABLE_GRAPH = new PropertyDescriptor.Builder()
            .name("Enable Graph")
            .description("Enables Graph table")
            .required(false)
            .defaultValue("True").allowableValues("True","False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor INDEX_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Index Table Name")
            .description("Index table name")
            .required(true)
            .defaultValue("shardIndex")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor GRAPH_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Graph Table Name")
            .description("Graph table name")
            .required(true)
            .defaultValue("graph")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor REVERSE_INDEX_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Reverse Index Table Name")
            .description("Reverse Index table name")
            .required(true)
            .defaultValue("shardReverseIndex")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor NUM_SHARD = new PropertyDescriptor.Builder()
            .name("Num shards")
            .description("Number of shards")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in Accumulo")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Accumulo")
            .build();

    protected final Multimap<String,Object> metadataForValidation = ArrayListMultimap.create(100, 1);

    // protected DataTypeHelperImpl dataTypeHelper;

    protected ConcurrentHashMap<String,DataTypeHelperImpl> datatypes = new ConcurrentHashMap<>();


    protected boolean requiredForValidation(String fieldName) {
        if (metadataForValidation.containsKey(fieldName)) {
            return false;
        }
        return false;
    }


    /**
     * Connector that we need to persist while we are operational.
     */
    protected AccumuloClient client;

    /**
     * Table writer that will close when we shutdown or upon error.
     */
    protected MultiTableBatchWriter tableWriter = null;


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String pdn) {
        String propertyDescriptorName = pdn;
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
        else if ( propertyDescriptorName.startsWith("FROM")){
            String[] parts = propertyDescriptorName.split("\\.");
            String displayName;
            String description;

            if (parts.length == 2) {
                displayName = String.format("%s FROM edge relationship", parts[1]);
                description = String.format("Specifies the from edge relationship", parts[1]);
            } else if ( parts.length == 3 && parts[2].equalsIgnoreCase("regex")) {
                propertyDescriptorName = "FROM." + parts[1] + ".regex";
                displayName = String.format("%s FROM edge regex relationship", parts[1]);
                description = String.format("Specifies the to edge relationship", parts[1]);
            } else {
                return null;
            }

            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(displayName)
                    .description(description)
                    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                    .dynamic(true)
                    .build();
        }
        else if ( propertyDescriptorName.startsWith("TO")){
            String[] parts = propertyDescriptorName.split("\\.");
            String displayName;
            String description;

            if (parts.length == 2) {
                displayName = String.format("%s TO edge relationship", parts[1]);
                description = String.format("Specifies the to edge relationship", parts[1]);
            } else if ( parts.length == 3 && parts[2].equalsIgnoreCase("regex")) {
                propertyDescriptorName = "TO." + parts[1] + ".regex";
                displayName = String.format("%s TO edge regex relationship", parts[1]);
                description = String.format("Specifies the to edge relationship", parts[1]);
            } else {
                return null;
            }

            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(displayName)
                    .description(description)
                    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                    .dynamic(true)
                    .build();
        }

        return null;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(baseProperties);
        properties.add(DATA_NAME);
        properties.add(ENABLE_METADATA);
        properties.add(ENABLE_METRICS);
        properties.add(METRICS_RECEIVERS);
        properties.add(INDEXED_FIELDS);
        properties.add(INDEX_ALL_FIELDS);
        properties.add(INDEX_TABLE_NAME);
        properties.add(REVERSE_INDEX_TABLE_NAME);
        properties.add(NUM_SHARD);
        properties.add(ENABLE_GRAPH);
        properties.add(GRAPH_TABLE_NAME);
        properties.add(INGEST_HELPER);
        properties.add(RECORD_READER);
        properties.add(DATA_HANDLER_CLASS);
        properties.add(FATAL_ERRORS);
        properties.add(UUID_FIELDS);
        properties.add(LABELS_CONFIG);
        properties.add(METRICS_FIELDS);
        return properties;
    }


}

