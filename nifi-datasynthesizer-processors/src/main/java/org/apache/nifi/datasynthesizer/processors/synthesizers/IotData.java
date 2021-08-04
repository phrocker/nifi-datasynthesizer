package org.apache.nifi.datasynthesizer.processors.synthesizers;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizer;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class IotData extends DataSynthesizer {

    public IotData(){

    }


    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        sampler = new ThreadLocal<>();

//      for (String line : Resources.readLines(Resources.getResource(resourceName), Charsets.UTF_8)) {
 //           if (!line.startsWith("#")) {
        definedSchema = new Scanner(IotData.class.getResourceAsStream("/generators" + FileSystems.getDefault().getSeparator() + "iot-data.json"), "UTF-8").useDelimiter("\\A").next();


    }
}
