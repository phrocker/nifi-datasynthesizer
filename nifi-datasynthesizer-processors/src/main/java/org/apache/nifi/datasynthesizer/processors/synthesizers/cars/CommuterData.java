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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"data-synthesis", "synthetic", "record", "commuter", "vehicle", "geo"})
@CapabilityDescription("Simulates the daily drive of a commuter: a home location, a workplace a few miles away and a "
        + "trajectory of timestamped GPS points with vehicle telemetry. Home can be pinned to a zip code.")
public class CommuterData extends DataSynthesizerBase {

    private static final Pattern ZIP = Pattern.compile("^[0-9]{5}(?:-[0-9]{4})?$");
    /** The commuter sampler measures distance in kilometres. */
    static final double MILES_TO_KM = 1.609344;

    public static final PropertyDescriptor HOME_LOCATION = new PropertyDescriptor.Builder()
            .name("home-location")
            .displayName("Home Location")
            .description("Zip code the commuter lives in. Random when unset.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor WORK_DISTANCE = new PropertyDescriptor.Builder()
            .name("work-distance")
            .displayName("Work Distance")
            .description("Typical distance from home to work, in miles.")
            .defaultValue("5.0")
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(HOME_LOCATION, WORK_DISTANCE, RECORD_COUNT, RECORD_WRITER);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        resetSamplers();
        final String home = context.getProperty(HOME_LOCATION).isSet()
                ? "{\"class\": \"zip\", \"zip\": \"" + context.getProperty(HOME_LOCATION).getValue() + "\"}"
                : "{\"class\": \"zip\"}";
        definedSchema = "[{\"name\": \"commuter-data\", \"class\": \"commuter\", \"home\": " + home
                + ", \"work\": " + (context.getProperty(WORK_DISTANCE).asDouble() * MILES_TO_KM) + "}]";
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        if (context.getProperty(HOME_LOCATION).isSet()) {
            final String zip = context.getProperty(HOME_LOCATION).getValue();
            if (!ZIP.matcher(zip).matches()) {
                return List.of(new ValidationResult.Builder().subject(HOME_LOCATION.getDisplayName()).input(zip).valid(false)
                        .explanation("must be a five digit zip code, optionally followed by a hyphen and four digits").build());
            }
        }
        return List.of();
    }
}
