package org.apache.nifi.datasynthesizer.processors.synthesizers.iot;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.processor.ProcessContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"data-synthesis", "record", "iot"})
public class IotData extends DataSynthesizerBase {

    public IotData(){

    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties  = new ArrayList<>();
        properties.add(RECORD_COUNT);
        properties.add(RECORD_WRITER);
        return properties;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        sampler = new ThreadLocal<>();
        definedSchema = new Scanner(IotData.class.getResourceAsStream("/generators/iot-data.json"), "UTF-8").useDelimiter("\\A").next();


    }
}