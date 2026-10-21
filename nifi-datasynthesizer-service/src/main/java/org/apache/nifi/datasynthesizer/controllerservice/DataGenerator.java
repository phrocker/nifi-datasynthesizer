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
package org.apache.nifi.datasynthesizer.controllerservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.mapr.synth.samplers.SchemaSampler;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.datasynthesizer.llm.TextCompletionContext;
import org.apache.nifi.datasynthesizer.services.SchemaLookupService;
import org.apache.nifi.datasynthesizer.services.TextGenerationService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Tags({"datasynthesizer", "schema", "log-synth", "synthetic"})
@CapabilityDescription("Holds a catalog of named log-synth schemas so that many DataSynthesizer processors can share one "
        + "definition. Each dynamic property is a schema: the property name is the key and the value is either the "
        + "schema JSON itself or a path to a file containing it. Schemas that use the 'llm' sampler also need a "
        + "Text Generation Service.")
@DynamicProperty(name = "Schema name", value = "Schema JSON or a path to a schema file",
        description = "Registers a log-synth schema under the given name")
public class DataGenerator extends AbstractControllerService implements SchemaLookupService {

    public static final PropertyDescriptor TEXT_GENERATION_SERVICE = new PropertyDescriptor.Builder()
            .name("Text Generation Service")
            .description("Optional LLM-backed service used by any schema field with class 'llm'.")
            .identifiesControllerService(TextGenerationService.class)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(TEXT_GENERATION_SERVICE);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Map<String, SchemaSampler> samplers = new ConcurrentHashMap<>();
    private volatile TextGenerationService textGenerationService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("log-synth schema JSON, or a path to a file containing it")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .required(false)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();
        context.getProperties().forEach((descriptor, value) -> {
            if (!descriptor.isDynamic() || value == null) {
                return;
            }
            try {
                parse(value);
            } catch (final Exception e) {
                results.add(new ValidationResult.Builder()
                        .subject(descriptor.getName())
                        .valid(false)
                        .explanation("not a valid log-synth schema or readable schema file: " + e.getMessage())
                        .build());
            }
        });
        return results.isEmpty() ? Collections.emptyList() : results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        samplers.clear();
        textGenerationService = context.getProperty(TEXT_GENERATION_SERVICE).isSet()
                ? context.getProperty(TEXT_GENERATION_SERVICE).asControllerService(TextGenerationService.class)
                : null;
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic() || entry.getValue() == null) {
                continue;
            }
            try {
                samplers.put(entry.getKey().getName(), parse(entry.getValue()));
            } catch (final IOException e) {
                throw new InitializationException("Unable to load schema '" + entry.getKey().getName() + "'", e);
            }
        }
        getLogger().info("Loaded {} schemas", samplers.size());
    }

    @OnDisabled
    public void onDisabled() {
        samplers.clear();
        textGenerationService = null;
    }

    @Override
    public Optional<JsonNode> sampleSchema(final String key) {
        final SchemaSampler sampler = samplers.get(key);
        if (sampler == null) {
            return Optional.empty();
        }
        final TextGenerationService service = textGenerationService;
        if (service == null) {
            return Optional.of(sampler.sample());
        }
        try (TextCompletionContext.Scope ignored = TextCompletionContext.use(TextGenerationAdapter.adapt(service))) {
            return Optional.of(sampler.sample());
        }
    }

    @Override
    public boolean hasSchema(final String key) {
        return samplers.containsKey(key);
    }

    static SchemaSampler parse(final String value) throws IOException {
        final String definition = looksLikeJson(value) ? value : Files.readString(Path.of(value.trim()));
        try {
            return new SchemaSampler(definition);
        } catch (final MismatchedInputException e) {
            return new SchemaSampler("[" + definition + "]");
        }
    }

    private static boolean looksLikeJson(final String value) {
        final String trimmed = value.trim();
        if (!(trimmed.startsWith("[") || trimmed.startsWith("{"))) {
            return false;
        }
        try {
            MAPPER.readTree(trimmed);
            return true;
        } catch (final IOException e) {
            return false;
        }
    }
}
