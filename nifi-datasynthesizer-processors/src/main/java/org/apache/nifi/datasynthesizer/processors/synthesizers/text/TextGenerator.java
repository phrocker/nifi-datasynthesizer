package org.apache.nifi.datasynthesizer.processors.synthesizers.text;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.datasynthesizer.processors.data.MarkovChain;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"data-synthesis", "record", "text"})
public class TextGenerator extends DataSynthesizerBase {

    private MarkovChain textGen;


    protected static final PropertyDescriptor WORD_LENGTH = new PropertyDescriptor.Builder()
            .name("word-length")
            .displayName("word Length")
            .description("Length of expected text")
            .defaultValue("128")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();
    private Integer textLength = 128;

    public TextGenerator(){

    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties  = new ArrayList<>();
        properties.add(RECORD_COUNT);
        properties.add(WORD_LENGTH);
        properties.add(RECORD_WRITER);
        return properties;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        sampler = new ThreadLocal<>();
        try {
            textGen = new MarkovChain(TextGenerator.class.getResourceAsStream("/trainingset/alice_oz.txt"));
        } catch (IOException e) {
            throw new InstantiationException(e.getMessage());
        }
        textLength = context.getProperty(WORD_LENGTH).asInteger();
        if (textLength < 0){
            textLength=1;
        }
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        int recordCount = processContext.getProperty(RECORD_COUNT).asInteger();
        try {
            definedSchema = "[" +
                    "{\"name\":\"message\", \"class\":\"text\", \"text\":\"" + textGen.produce(textLength)  + "\"}" +
                    "]";
        } catch (IOException e) {
            throw new ProcessException(e.getMessage());
        }
        sampler.set(null);
        createRecords(processSession,processContext,recordCount);
    }
}
