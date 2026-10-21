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
package org.apache.nifi.datasynthesizer.processors.synthesizers.text;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.datasynthesizer.processors.synthesizers.telephony.TextMessage;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"data-synthesis", "synthetic", "record", "text", "markov", "llm"})
@CapabilityDescription("Generates records containing a block of free text. Text comes from an offline Markov chain "
        + "trained on a bundled corpus, or from the configured Text Generation Service when one is set.")
public class TextGenerator extends DataSynthesizerBase {

    static final AllowableValue ALICE_OZ = new AllowableValue("alice_oz", "Alice in Wonderland and The Wizard of Oz", "Narrative prose");
    static final AllowableValue US_CONST = new AllowableValue("us_const", "US Constitution", "Formal legal prose");

    public static final PropertyDescriptor WORD_LENGTH = new PropertyDescriptor.Builder()
            .name("word-length")
            .displayName("Word Length")
            .description("Approximate length of the generated text, in words.")
            .defaultValue("128")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor CORPUS = new PropertyDescriptor.Builder()
            .name("corpus")
            .displayName("Corpus")
            .description("Training corpus for the offline Markov chain.")
            .allowableValues(ALICE_OZ, US_CONST)
            .defaultValue(ALICE_OZ.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor LLM_PROMPT = new PropertyDescriptor.Builder()
            .name("llm-prompt")
            .displayName("LLM Prompt")
            .description("Prompt used when a Text Generation Service is configured.")
            .defaultValue("Write a short passage of plausible but entirely fictional prose, such as a product review, "
                    + "a support ticket, a news blurb or a diary entry. Vary the genre.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor LLM_BATCH = new PropertyDescriptor.Builder()
            .name("llm-batch")
            .displayName("LLM Batch Size")
            .description("Number of texts requested from the Text Generation Service per API call.")
            .defaultValue("5")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = withChaos(List.of(
            WORD_LENGTH, CORPUS, RECORD_COUNT, RECORD_WRITER, TEXT_GENERATION_SERVICE, LLM_PROMPT, LLM_BATCH));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        resetSamplers();
        final int words = context.getProperty(WORD_LENGTH).asInteger();
        if (context.getProperty(TEXT_GENERATION_SERVICE).isSet()) {
            final String prompt = context.getProperty(LLM_PROMPT).getValue() + " Aim for about " + words + " words.";
            definedSchema = "[{\"name\": \"message\", \"class\": \"llm\", \"prompt\": " + TextMessage.quote(prompt)
                    + ", \"maxTokens\": " + Math.max(512, words * 4)
                    + ", \"batch\": " + context.getProperty(LLM_BATCH).asInteger() + "}]";
        } else {
            definedSchema = "[{\"name\": \"message\", \"class\": \"markov\", \"corpus\": \""
                    + context.getProperty(CORPUS).getValue() + "\", \"length\": " + words + "}]";
        }
    }
}
