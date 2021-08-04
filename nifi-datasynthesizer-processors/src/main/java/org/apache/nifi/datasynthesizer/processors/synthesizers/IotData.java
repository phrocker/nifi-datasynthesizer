package org.apache.nifi.datasynthesizer.processors.synthesizers;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizer;
import org.apache.nifi.processor.ProcessContext;

import java.nio.file.FileSystems;
import java.util.Scanner;

public class IotData extends DataSynthesizer {

    public IotData(){

    }


    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        sampler = new ThreadLocal<>();
        definedSchema = new Scanner(IotData.class.getResourceAsStream("/generators" + FileSystems.getDefault().getSeparator() + "iot-data.json"), "UTF-8").useDelimiter("\\A").next();


    }
}
