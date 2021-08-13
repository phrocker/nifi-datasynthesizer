package org.apache.nifi.datasynthesizer.processors.synthesizers.telephony;

import com.github.javafaker.Faker;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.datasynthesizer.processors.synthesizers.text.MarkovChain;
import org.apache.nifi.datasynthesizer.processors.synthesizers.text.TextGenerator;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"data-synthesis", "record", "text", "phone"})
public class TextMessage extends DataSynthesizerBase {
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

    protected static final PropertyDescriptor WORD_LENGTH = new PropertyDescriptor.Builder()
            .name("word-length")
            .displayName("word Length")
            .description("Length of expected text")
            .defaultValue("160")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();

    Faker faker = new Faker();
    private MarkovChain textGen;


    public TextMessage(){

    }


    @OnScheduled
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
        sampler = new ThreadLocal<>();
        try {
            textGen = new MarkovChain(TextGenerator.class.getResourceAsStream("/trainingset/us_const.txt"));
        } catch (IOException e) {
            throw new InstantiationException(e.getMessage());
        }
        Integer textLength = context.getProperty(WORD_LENGTH).asInteger();
        try {
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
                    "{\"name\":\"message\", \"class\":\"text\", \"text\":\"" + StringEscapeUtils.escapeJava(textGen.produceText(textLength))  + "\"}" +
                    "]";
        } catch (IOException e) {
            throw new InstantiationException(e.getMessage());
        }


    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties  = new ArrayList<>();
        properties.add(SOURCE_AREA_CODE);
        properties.add(DESTINATION_AREA_CODE);
        properties.add(RECORD_COUNT);
        properties.add(RECORD_WRITER);
        properties.add(WORD_LENGTH);
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
