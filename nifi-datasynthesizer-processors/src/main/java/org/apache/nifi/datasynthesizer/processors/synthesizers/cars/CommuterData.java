/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.datasynthesizer.processors.synthesizers.cars;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.regex.Pattern;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"data-synthesis", "record", "commuter"})
public class CommuterData extends DataSynthesizerBase {

    protected static final PropertyDescriptor HOME_LOCATION = new PropertyDescriptor.Builder()
            .name("home-location")
            .displayName("Home Location")
            .description("Specifies an area code")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();



    public CommuterData(){

    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties  = new ArrayList<>();
        properties.add(HOME_LOCATION);
        properties.add(RECORD_COUNT);
        properties.add(RECORD_WRITER);
        return properties;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        sampler = new ThreadLocal<>();
        String zipLine =  "          \"home\": { \"name\": \"loc\", \"class\": \"zip\"\n },";
        if (context.getProperty(HOME_LOCATION).isSet()) {
            zipLine =  "          \"home\": { \"name\": \"loc\", \"class\": \"zip\", \"zip\": \"" + context.getProperty(HOME_LOCATION).getValue() +  "\"},";
        }
        definedSchema = "[{\"name\": \"commuter-data\", \"class\": \"commuter\", " +
               zipLine +
                "          \"work\": 5.0 " +
                "    }]";


    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = Collections.emptySet();
        if (validationContext.getProperty(HOME_LOCATION).isSet()){
            final String homeZipCode = validationContext.getProperty(HOME_LOCATION).getValue();
            String zipRegex = "^[0-9]{5}(?:-[0-9]{4})?$";
            if (! Pattern.matches(zipRegex,homeZipCode)){
                set.add(new ValidationResult.Builder().valid(false).explanation("Zip code must be valid, either five digits or five digits followed by a hyphen and four digits").input(homeZipCode).subject("Home Location").build());
            }
        }
        return set;
    }
}
