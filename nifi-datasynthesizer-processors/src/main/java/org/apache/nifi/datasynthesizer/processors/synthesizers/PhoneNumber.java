package org.apache.nifi.datasynthesizer.processors.synthesizers;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"data-synthesis", "record", "phone"})
public class PhoneNumber extends DataSynthesizerBase {
    protected static final PropertyDescriptor AREA_CODE = new PropertyDescriptor.Builder()
            .name("area-code")
            .displayName("Area Code")
            .description("Specifies an area code")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();



    public PhoneNumber(){

    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        sampler = new ThreadLocal<>();
        int area_min = 100;
        int area_max = 999;
        if (context.getProperty(AREA_CODE).isSet()){
            final Integer value = context.getProperty(AREA_CODE).asInteger();
            area_min = value;
            area_max = value;
        }
        definedSchema = "[{\"name\": \"phone_number\", \"class\": \"join\", \"separator\": \"-\", \"value\": {\n" +
                "          \"class\":\"sequence\",\n" +
                "          \"length\":3,\n" +
                "          \"array\":[\n" +
                "              { \"class\": \"int\", \"min\": " +String.valueOf(area_min) + ", \"max\": " + String.valueOf(area_max) + "},\n" +
                "              { \"class\": \"int\", \"min\": 100, \"max\": 999},\n" +
                "              { \"class\": \"int\", \"min\": 1000, \"max\": 9999}\n" +
                "          ]\n" +
                "    }}]";


    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties  = new ArrayList<>();
        properties.add(AREA_CODE);
        properties.add(RECORD_COUNT);
        properties.add(RECORD_WRITER);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = Collections.emptySet();
        if (validationContext.getProperty(AREA_CODE).isSet()){
            final String strVal = validationContext.getProperty(AREA_CODE).getValue();
            final Integer value = validationContext.getProperty(AREA_CODE).asInteger();
            if (value < 0 || value > 999){
                set.add(new ValidationResult.Builder().valid(false).explanation("Area code must be a 3 digit number").input(strVal).subject("Area code").build());
            }
        }
        return set;
    }
}
