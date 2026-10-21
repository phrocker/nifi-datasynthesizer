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
@Tags({"data-synthesis", "synthetic", "record", "sms", "text", "phone", "llm"})
@CapabilityDescription("Generates SMS text message records: source IMEI, source and destination phone numbers, a "
        + "timestamp and a message body. The body comes from an offline Markov chain, or from the configured Text "
        + "Generation Service when one is set, in which case messages are requested in batches of 'LLM Batch Size'.")
public class TextMessage extends DataSynthesizerBase {

    public static final PropertyDescriptor SOURCE_AREA_CODE = new PropertyDescriptor.Builder()
            .name("origin-area-code")
            .displayName("Source Area Code")
            .description("Area code of the sending number. Random when unset.")
            .addValidator(StandardValidators.createLongValidator(100, 999, true))
            .required(false)
            .build();

    public static final PropertyDescriptor DESTINATION_AREA_CODE = new PropertyDescriptor.Builder()
            .name("destination-area-code")
            .displayName("Destination Area Code")
            .description("Area code of the receiving number. Random when unset.")
            .addValidator(StandardValidators.createLongValidator(100, 999, true))
            .required(false)
            .build();

    public static final PropertyDescriptor WORD_LENGTH = new PropertyDescriptor.Builder()
            .name("word-length")
            .displayName("Message Length")
            .description("Approximate message length in words.")
            .defaultValue("25")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor LLM_PROMPT = new PropertyDescriptor.Builder()
            .name("llm-prompt")
            .displayName("LLM Prompt")
            .description("Prompt used when a Text Generation Service is configured.")
            .defaultValue("Write one realistic SMS text message between two people. Vary the topic, tone and length; "
                    + "most are casual and short.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor LLM_BATCH = new PropertyDescriptor.Builder()
            .name("llm-batch")
            .displayName("LLM Batch Size")
            .description("Number of messages requested from the Text Generation Service per API call.")
            .defaultValue("20")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            SOURCE_AREA_CODE, DESTINATION_AREA_CODE, WORD_LENGTH, RECORD_COUNT, RECORD_WRITER,
            TEXT_GENERATION_SERVICE, LLM_PROMPT, LLM_BATCH);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        resetSamplers();
        final Integer source = context.getProperty(SOURCE_AREA_CODE).isSet() ? context.getProperty(SOURCE_AREA_CODE).asInteger() : null;
        final Integer destination = context.getProperty(DESTINATION_AREA_CODE).isSet() ? context.getProperty(DESTINATION_AREA_CODE).asInteger() : null;
        final int words = context.getProperty(WORD_LENGTH).asInteger();

        final String message;
        if (context.getProperty(TEXT_GENERATION_SERVICE).isSet()) {
            final String prompt = context.getProperty(LLM_PROMPT).getValue() + " Keep it under " + words + " words.";
            message = "{\"name\": \"message\", \"class\": \"llm\", \"prompt\": " + quote(prompt)
                    + ", \"maxTokens\": " + Math.max(32, words * 3)
                    + ", \"batch\": " + context.getProperty(LLM_BATCH).asInteger() + "}";
        } else {
            message = "{\"name\": \"message\", \"class\": \"markov\", \"corpus\": \"us_const\", \"length\": " + words + "}";
        }
        definedSchema = "["
                + "{\"name\": \"source_imei\", \"class\": \"imei\"},"
                + PhoneNumber.phoneField("source_phone_number", source) + ","
                + PhoneNumber.phoneField("destination_phone_number", destination) + ","
                + "{\"name\": \"timestamp\", \"class\": \"date\", \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"},"
                + message
                + "]";
    }

    public static String quote(final String text) {
        return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
