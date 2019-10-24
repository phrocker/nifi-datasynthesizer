package org.poma.accumulo.nifi.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.poma.accumulo.nifi.controllerservices.BaseAccumuloService;

import java.util.ArrayList;
import java.util.List;

/**
 * Base Accumulo class that provides connector services, table name, and thread
 * properties
 */
public abstract class BaseAccumuloProcessor extends AbstractProcessor {

    protected static final PropertyDescriptor ACCUMULO_CONNECTOR_SERVICE = new PropertyDescriptor.Builder()
            .name("Accumulo Connector Service")
            .description("Specifies the Controller Service to use for accessing Accumulo.")
            .required(true)
            .identifiesControllerService(BaseAccumuloService.class)
            .build();


    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Accumulo Table into which data will be placed")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CREATE_TABLE = new PropertyDescriptor.Builder()
            .name("Create Table")
            .description("Creates a table if it does not exist. This property will only be used when EL is not present in 'Table Name'")
            .required(true)
            .defaultValue("False")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    protected static final PropertyDescriptor THREADS = new PropertyDescriptor.Builder()
            .name("Threads")
            .description("Number of threads used for reading and writing")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ACCUMULO_CONNECTOR_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(CREATE_TABLE);
        properties.add(THREADS);
        return properties;
    }



}
