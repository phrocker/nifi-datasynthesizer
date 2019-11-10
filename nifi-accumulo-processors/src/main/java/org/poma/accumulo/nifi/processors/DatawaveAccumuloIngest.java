package org.poma.accumulo.nifi.processors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.validation.EventValidator;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.poma.accumulo.nifi.controllerservices.BaseAccumuloService;

import java.util.List;


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
public abstract class DatawaveAccumuloIngest extends BaseAccumuloProcessor {
    protected static final PropertyDescriptor DATA_NAME = new PropertyDescriptor.Builder()
            .name("Data name")
            .description("Data type name of the data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor INGEST_HELPER = new PropertyDescriptor.Builder()
            .name("Ingest Helper")
            .description("Ingest Helper class")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Hadoop Record reader class")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DATA_HANDLER_CLASS = new PropertyDescriptor.Builder()
            .name("Handler Class")
            .description("Datawave handler class")
            .required(true)
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


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in Accumulo")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Accumulo")
            .build();

    protected final Multimap<String,Object> metadataForValidation = ArrayListMultimap.create(100, 1);

    protected DataTypeHelperImpl dataTypeHelper;


    protected boolean requiredForValidation(String fieldName) {
        if (metadataForValidation.containsKey(fieldName)) {
            return false;
        }
        return false;
    }

    protected DataTypeHelperImpl getHelper(){
        return dataTypeHelper;
    }


    /**
     * Connector service which provides us a connector if the configuration is correct.
     */
    protected BaseAccumuloService accumuloConnectorService;

    /**
     * Connector that we need to persist while we are operational.
     */
    protected Connector connector;

    /**
     * Table writer that will close when we shutdown or upon error.
     */
    protected MultiTableBatchWriter tableWriter = null;


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(DATA_NAME);
        properties.add(INGEST_HELPER);
        properties.add(RECORD_READER);
        properties.add(DATA_HANDLER_CLASS);
        properties.add(FATAL_ERRORS);
        properties.add(UUID_FIELDS);
        return properties;
    }


}
