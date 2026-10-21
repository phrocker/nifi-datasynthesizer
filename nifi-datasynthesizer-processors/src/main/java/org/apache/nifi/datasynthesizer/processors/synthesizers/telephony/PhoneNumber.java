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
package org.apache.nifi.datasynthesizer.processors.synthesizers.telephony;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"data-synthesis", "synthetic", "record", "phone"})
@CapabilityDescription("Generates North American phone number records, optionally pinned to one area code.")
public class PhoneNumber extends DataSynthesizerBase {

    public static final PropertyDescriptor AREA_CODE = new PropertyDescriptor.Builder()
            .name("area-code")
            .displayName("Area Code")
            .description("Three digit area code. Random when unset.")
            .addValidator(StandardValidators.createLongValidator(100, 999, true))
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = withChaos(List.of(AREA_CODE, RECORD_COUNT, RECORD_WRITER));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        resetSamplers();
        final Integer areaCode = context.getProperty(AREA_CODE).isSet() ? context.getProperty(AREA_CODE).asInteger() : null;
        definedSchema = "[" + phoneField("phone_number", areaCode) + "]";
    }

    /** log-synth fragment for a NANP-style phone number field, optionally fixed to one area code. */
    static String phoneField(final String name, final Integer areaCode) {
        final int min = areaCode == null ? 200 : areaCode;
        final int max = areaCode == null ? 999 : areaCode;
        return "{\"name\": \"" + name + "\", \"class\": \"join\", \"separator\": \"-\", \"value\": {"
                + "\"class\": \"sequence\", \"length\": 3, \"array\": ["
                + "{\"class\": \"int\", \"min\": " + min + ", \"max\": " + max + "},"
                + "{\"class\": \"int\", \"min\": 200, \"max\": 999},"
                + "{\"class\": \"int\", \"min\": 1000, \"max\": 9999}]}}";
    }
}
