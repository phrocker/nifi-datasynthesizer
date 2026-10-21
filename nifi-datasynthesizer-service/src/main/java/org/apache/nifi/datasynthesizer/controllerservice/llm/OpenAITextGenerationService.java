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
package org.apache.nifi.datasynthesizer.controllerservice.llm;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.errors.OpenAIException;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessage;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.datasynthesizer.services.TextGenerationRequest;
import org.apache.nifi.datasynthesizer.services.TextGenerationService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({"openai", "gpt", "ollama", "vllm", "llm", "text", "generation", "datasynthesizer"})
@CapabilityDescription("Generates text through the OpenAI Chat Completions API using the official OpenAI Java SDK. "
        + "Point Base URL at any OpenAI-compatible server (Ollama, vLLM, LM Studio, Azure OpenAI gateways) to use "
        + "local or third-party models. Used by DataSynthesizer processors for schema fields of class 'llm'.")
public class OpenAITextGenerationService extends AbstractControllerService implements TextGenerationService {

    public static final String DEFAULT_BASE_URL = "https://api.openai.com/v1";
    public static final String DEFAULT_MODEL = "gpt-5-mini";

    public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("API key. Local OpenAI-compatible servers usually accept any non-empty value.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor BASE_URL = new PropertyDescriptor.Builder()
            .name("Base URL")
            .description("Chat Completions base URL, for example http://localhost:11434/v1 for Ollama.")
            .required(true)
            .defaultValue(DEFAULT_BASE_URL)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MODEL = new PropertyDescriptor.Builder()
            .name("Model")
            .description("Model name as understood by the endpoint.")
            .required(true)
            .defaultValue(DEFAULT_MODEL)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Request Timeout")
            .description("Maximum time to wait for one completion.")
            .required(true)
            .defaultValue("60 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_RETRIES = new PropertyDescriptor.Builder()
            .name("Max Retries")
            .description("Retries for rate limits, server errors and connection failures.")
            .required(true)
            .defaultValue("2")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(API_KEY, BASE_URL, MODEL, TIMEOUT, MAX_RETRIES);

    private volatile OpenAIClient client;
    private volatile String model;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        client = OpenAIOkHttpClient.builder()
                .apiKey(context.getProperty(API_KEY).getValue())
                .baseUrl(context.getProperty(BASE_URL).evaluateAttributeExpressions().getValue())
                .timeout(Duration.ofMillis(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)))
                .maxRetries(context.getProperty(MAX_RETRIES).asInteger())
                .build();
        model = context.getProperty(MODEL).evaluateAttributeExpressions().getValue();
    }

    @OnDisabled
    public void onDisabled() {
        final OpenAIClient current = client;
        client = null;
        if (current != null) {
            current.close();
        }
    }

    @Override
    public String generate(final TextGenerationRequest request) throws IOException {
        final OpenAIClient current = client;
        if (current == null) {
            throw new IOException("Service is not enabled");
        }
        final ChatCompletionCreateParams.Builder params = ChatCompletionCreateParams.builder()
                .model(model)
                .maxCompletionTokens(request.maxTokens());
        request.systemPrompt().ifPresent(params::addSystemMessage);
        params.addUserMessage(request.prompt());
        request.samplingTemperature().ifPresent(params::temperature);

        final ChatCompletion completion;
        try {
            completion = current.chat().completions().create(params.build());
        } catch (final OpenAIException e) {
            throw new IOException("OpenAI-compatible request failed: " + e.getMessage(), e);
        }
        if (completion.choices().isEmpty()) {
            throw new IOException("Endpoint returned no choices");
        }
        final ChatCompletion.Choice choice = completion.choices().get(0);
        if ("length".equals(choice.finishReason().toString())) {
            getLogger().warn("Reply was cut off at {} tokens; raise maxTokens on the llm field", request.maxTokens());
        }
        final ChatCompletionMessage message = choice.message();
        if (message.refusal().isPresent()) {
            throw new IOException("Model declined the request: " + message.refusal().get());
        }
        final String text = message.content().orElse("");
        if (text.isBlank()) {
            throw new IOException("Endpoint returned no text (finish reason "
                    + completion.choices().get(0).finishReason() + ")");
        }
        return text;
    }
}
