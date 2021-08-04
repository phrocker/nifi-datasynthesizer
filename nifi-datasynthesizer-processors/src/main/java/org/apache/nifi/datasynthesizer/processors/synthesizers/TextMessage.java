package org.apache.nifi.datasynthesizer.processors.synthesizers;

import com.github.javafaker.Faker;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizer;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TextMessage extends DataSynthesizer {
    protected static final PropertyDescriptor SOURCE_AREA_CODE = new PropertyDescriptor.Builder()
            .name("origin-area-code")
            .displayName("Source Area Code")
            .description("Specifies an area code for the source phone number")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();

    protected static final PropertyDescriptor DESTINATION_AREA_CODE = new PropertyDescriptor.Builder()
            .name("destination-area-code")
            .displayName("Destination Area Code")
            .description("Specifies an area code for the destination phone number")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();

    Faker faker = new Faker();


    public TextMessage(){

    }


    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        sampler = new ThreadLocal<>();
        int source_area_min = 100;
        int source_area_max = 999;
        int destination_area_min = 100;
        int destination_area_max = 999;
        if (context.getProperty(SOURCE_AREA_CODE).isSet()){
            final Integer value = context.getProperty(SOURCE_AREA_CODE).asInteger();
            source_area_min = value;
            source_area_max = value;
        }
        if (context.getProperty(DESTINATION_AREA_CODE).isSet()){
            final Integer value = context.getProperty(DESTINATION_AREA_CODE).asInteger();
            destination_area_min = value;
            destination_area_max = value;
        }
        definedSchema = "[" +
            "{\"name\":\"source_imei\", \"class\":\"imei\"}," +
            "{\"name\": \"source_phone_number\", \"class\": \"join\", \"separator\": \"-\", \"value\": {\n" +
                "          \"class\":\"sequence\",\n" +
                "          \"length\":3,\n" +
                "          \"array\":[\n" +
                "              { \"class\": \"int\", \"min\": " +String.valueOf(source_area_min) + ", \"max\": " + String.valueOf(source_area_max) + "},\n" +
                "              { \"class\": \"int\", \"min\": 100, \"max\": 999},\n" +
                "              { \"class\": \"int\", \"min\": 1000, \"max\": 9999}\n" +
                "          ]\n" +
                "    }}," +
                "{\"name\": \"destination_phone_number\", \"class\": \"join\", \"separator\": \"-\", \"value\": {\n" +
                "          \"class\":\"sequence\",\n" +
                "          \"length\":3,\n" +
                "          \"array\":[\n" +
                "              { \"class\": \"int\", \"min\": " +String.valueOf(destination_area_min) + ", \"max\": " + String.valueOf(destination_area_max) + "},\n" +
                "              { \"class\": \"int\", \"min\": 100, \"max\": 999},\n" +
                "              { \"class\": \"int\", \"min\": 1000, \"max\": 9999}\n" +
                "          ]\n" +
                "    }}," +
                "{\"name\":\"sent_date_time\", \"class\":\"date\", \"format\":\"yyyy-MM-dd HH:mm:ssZ\"}," +
                "{\"name\":\"message\", \"class\":\"text\", \"text\":\"" + faker.backToTheFuture().quote()  + "\"}" +
                "]";


    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(SOURCE_AREA_CODE);
        properties.add(DESTINATION_AREA_CODE);

        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = Collections.emptySet();
        if (validationContext.getProperty(SOURCE_AREA_CODE).isSet()){
            final String strVal = validationContext.getProperty(SOURCE_AREA_CODE).getValue();
            final Integer value = validationContext.getProperty(SOURCE_AREA_CODE).asInteger();
            if (value < 0 || value > 999){
                set.add(new ValidationResult.Builder().valid(false).explanation("Area code must be a 3 digit number").input(strVal).subject("Area code").build());
            }
        }

        if (validationContext.getProperty(DESTINATION_AREA_CODE).isSet()){
            final String strVal = validationContext.getProperty(DESTINATION_AREA_CODE).getValue();
            final Integer value = validationContext.getProperty(DESTINATION_AREA_CODE).asInteger();
            if (value < 0 || value > 999){
                set.add(new ValidationResult.Builder().valid(false).explanation("Area code must be a 3 digit number").input(strVal).subject("Area code").build());
            }
        }
        return set;
    }
}
