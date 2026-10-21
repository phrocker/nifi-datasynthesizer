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

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.errors.AnthropicException;
import com.anthropic.models.beta.AnthropicBeta;
import com.anthropic.models.beta.messages.BetaContentBlock;
import com.anthropic.models.beta.messages.BetaFallbacksParam;
import com.anthropic.models.beta.messages.BetaMessage;
import com.anthropic.models.beta.messages.BetaOutputConfig;
import com.anthropic.models.beta.messages.BetaStopReason;
import com.anthropic.models.beta.messages.MessageCreateParams;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Tags({"anthropic", "claude", "llm", "text", "generation", "datasynthesizer"})
@CapabilityDescription("Generates text with Anthropic Claude models through the official Anthropic Java SDK. Used by "
        + "DataSynthesizer processors for schema fields of class 'llm'. Refusal fallbacks are enabled by default so a "
        + "declined request is retried server side on a model chosen for the refusal category.")
public class AnthropicTextGenerationService extends AbstractControllerService implements TextGenerationService {

    public static final String DEFAULT_MODEL = "claude-opus-5-5";

    public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("Anthropic API key.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODEL = new PropertyDescriptor.Builder()
            .name("Model")
            .description("Claude model ID. claude-opus-5-5 is the default; claude-haiku-5-5 is the budget option for "
                    + "high-volume synthetic text.")
            .required(true)
            .defaultValue(DEFAULT_MODEL)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final AllowableValue EFFORT_LOW = new AllowableValue("low", "Low", "Fastest and cheapest; fine for short synthetic text");
    static final AllowableValue EFFORT_MEDIUM = new AllowableValue("medium", "Medium", "Model default");
    static final AllowableValue EFFORT_HIGH = new AllowableValue("high", "High", "More reasoning per request");
    static final AllowableValue EFFORT_XHIGH = new AllowableValue("xhigh", "Extra High", "Deep reasoning");
    static final AllowableValue EFFORT_MAX = new AllowableValue("max", "Max", "Maximum reasoning, highest cost");

    public static final PropertyDescriptor EFFORT = new PropertyDescriptor.Builder()
            .name("Effort")
            .description("Reasoning effort. Low keeps synthetic text generation fast and inexpensive.")
            .required(true)
            .allowableValues(EFFORT_LOW, EFFORT_MEDIUM, EFFORT_HIGH, EFFORT_XHIGH, EFFORT_MAX)
            .defaultValue(EFFORT_LOW.getValue())
            .build();

    public static final PropertyDescriptor REFUSAL_FALLBACKS = new PropertyDescriptor.Builder()
            .name("Refusal Fallbacks")
            .description("When true, a request the model declines is retried server side on a fallback model chosen by "
                    + "refusal category (Anthropic's 'default' fallback routing).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
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

    public static final PropertyDescriptor BASE_URL = new PropertyDescriptor.Builder()
            .name("Base URL")
            .description("Override the API base URL, for a proxy or gateway. Leave empty for api.anthropic.com.")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            API_KEY, MODEL, EFFORT, REFUSAL_FALLBACKS, TIMEOUT, MAX_RETRIES, BASE_URL);

    private volatile AnthropicClient client;
    private volatile String model;
    private volatile BetaOutputConfig.Effort effort;
    private volatile boolean fallbacks;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final AnthropicOkHttpClient.Builder builder = AnthropicOkHttpClient.builder()
                .apiKey(context.getProperty(API_KEY).getValue())
                .timeout(Duration.ofMillis(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)))
                .maxRetries(context.getProperty(MAX_RETRIES).asInteger());
        if (context.getProperty(BASE_URL).isSet()) {
            builder.baseUrl(context.getProperty(BASE_URL).evaluateAttributeExpressions().getValue());
        }
        client = builder.build();
        model = context.getProperty(MODEL).evaluateAttributeExpressions().getValue();
        effort = BetaOutputConfig.Effort.of(context.getProperty(EFFORT).getValue());
        fallbacks = context.getProperty(REFUSAL_FALLBACKS).asBoolean();
    }

    @OnDisabled
    public void onDisabled() {
        final AnthropicClient current = client;
        client = null;
        if (current != null) {
            current.close();
        }
    }

    @Override
    public String generate(final TextGenerationRequest request) throws IOException {
        final AnthropicClient current = client;
        if (current == null) {
            throw new IOException("Service is not enabled");
        }
        final MessageCreateParams.Builder params = MessageCreateParams.builder()
                .model(model)
                .maxTokens(request.maxTokens())
                .outputConfig(BetaOutputConfig.builder().effort(effort).build())
                .addUserMessage(request.prompt());
        request.systemPrompt().ifPresent(params::system);
        // Sampling parameters are rejected by current Claude models, so the request temperature is not forwarded.
        if (fallbacks) {
            params.addBeta(AnthropicBeta.SERVER_SIDE_FALLBACK_2026_07_01).fallbacks(BetaFallbacksParam.ofDefault());
        }

        final BetaMessage message;
        try {
            message = current.beta().messages().create(params.build());
        } catch (final AnthropicException e) {
            throw new IOException("Anthropic request failed: " + e.getMessage(), e);
        }

        final Optional<BetaStopReason> stop = message.stopReason();
        if (stop.isPresent() && BetaStopReason.REFUSAL.equals(stop.get())) {
            final String detail = message.stopDetails()
                    .map(d -> d.category().map(Object::toString).orElse("unknown")
                            + d.explanation().map(x -> ": " + x).orElse(""))
                    .orElse("no details");
            throw new IOException("Anthropic declined the request (" + detail + ")");
        }
        final String text = message.content().stream()
                .map(BetaContentBlock::text)
                .filter(Optional::isPresent)
                .map(block -> block.get().text())
                .collect(Collectors.joining());
        if (text.isBlank()) {
            throw new IOException("Anthropic returned no text (stop reason " + stop.map(Object::toString).orElse("none") + ")");
        }
        return text;
    }
}
