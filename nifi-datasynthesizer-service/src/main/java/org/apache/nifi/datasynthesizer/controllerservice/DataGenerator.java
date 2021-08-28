package org.apache.nifi.datasynthesizer.controllerservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mapr.synth.samplers.SchemaSampler;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.datasynthesizer.services.SchemaLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@RequiresInstanceClassLoading
@Tags({"datasynthesizer"})
@CapabilityDescription("Provides a mapping of data generators.")
@DynamicProperty(name = "Object Schema", value = "Schema Object", description = "Provides a file path to the schema or the schema object")
public class DataGenerator extends AbstractControllerService implements SchemaLookupService {


    public static final PropertyDescriptor RELOAD_INTERVAL = new PropertyDescriptor.Builder()
            .name("Reload Interval")
            .description("Time before looking for changes")
            .defaultValue("60 min")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private Map<String,String> dynamicProperties = new HashMap<>();

    private Map<String,SchemaSampler> samplers = new HashMap<>();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        final Map<String,String> newDynamicPropertyNames = new HashMap<>(dynamicProperties);
        if (newValue == null) {
            newDynamicPropertyNames.remove(descriptor.getName());
        } else if (oldValue == null) {    // Adding a new property
            newDynamicPropertyNames.put(descriptor.getName(),newValue);
        }

        this.dynamicProperties = Collections.unmodifiableMap(newDynamicPropertyNames);
    }

    private boolean isJson(final String potentialJson){
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode node = (JsonNode) objectMapper.readTree(potentialJson);
            return true;
        } catch (JsonProcessingException e) {
            // we may have a file
        }
        return false;
    }


    private String loadFile(String filePath) throws IOException {

        return Files.readString( Path.of(filePath));
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        getLogger().info("Starting properties file service");

        dynamicProperties.forEach((key,value) ->{
                    SchemaSampler mySampler;
                    try{
                        String definedSchema = isJson(value) ? value : loadFile(value);
                        try {
                            mySampler = new SchemaSampler(definedSchema);
                        }
                        catch(MismatchedInputException mie){
                            mySampler = new SchemaSampler("[" + definedSchema + "]");
                        }
                        samplers.put(key,mySampler);
                    }catch(final IOException e){
                        throw new UncheckedIOException(e);
                    }

                }
        );

    }


    @Override
    public Optional<JsonNode> sampleSchema(String key) {
        SchemaSampler sampler = samplers.get(key);
        if (Objects.isNull(sampler))
            return Optional.empty();
        else
            return Optional.of(samplers.get(key).sample());
    }
}
