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
package org.apache.nifi.datasynthesizer.samplers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mapr.synth.samplers.FieldSampler;
import org.apache.nifi.datasynthesizer.llm.TextCompletion;
import org.apache.nifi.datasynthesizer.llm.TextCompletionContext;
import org.apache.nifi.datasynthesizer.llm.TextCompletionException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Fills a field with text produced by a large language model.
 * <p>
 * Schema example:
 * <pre>
 * {"name": "message", "class": "llm",
 *  "prompt": "Write one casual text message between two friends about weekend plans.",
 *  "system": "You generate realistic but fictional test data.",
 *  "maxTokens": 80, "batch": 20, "temperature": 1.0}
 * </pre>
 * The provider is supplied at runtime through {@link TextCompletionContext}; in NiFi that is the Text Generation
 * Service configured on the processor or controller service.
 * <p>
 * With {@code batch} greater than one the sampler asks the model for that many distinct variants in a single
 * request, formatted as a JSON array, and hands them out one per sample. That divides API calls (and cost) by the
 * batch size. If the model does not return valid JSON the response is split on line breaks instead.
 * <p>
 * Thread safe.
 */
public class LlmTextSampler extends FieldSampler {

    static final String DEFAULT_SYSTEM = "You generate realistic but entirely fictional synthetic test data. "
            + "Never mention that the data is synthetic. Reply with the requested text only, no preamble.";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String prompt;
    private String system = DEFAULT_SYSTEM;
    private int maxTokens = 256;
    private int batch = 1;
    private Double temperature;

    private final Deque<String> buffered = new ArrayDeque<>();

    public LlmTextSampler() {
    }

    @SuppressWarnings("unused")
    public void setPrompt(final String prompt) {
        this.prompt = prompt;
    }

    @SuppressWarnings("unused")
    public void setSystem(final String system) {
        this.system = system;
    }

    @SuppressWarnings("unused")
    public void setMaxTokens(final int maxTokens) {
        if (maxTokens < 1) {
            throw new IllegalArgumentException("maxTokens must be at least 1");
        }
        this.maxTokens = maxTokens;
    }

    @SuppressWarnings("unused")
    public void setBatch(final int batch) {
        if (batch < 1) {
            throw new IllegalArgumentException("batch must be at least 1");
        }
        this.batch = batch;
    }

    @SuppressWarnings("unused")
    public void setTemperature(final Double temperature) {
        this.temperature = temperature;
    }

    public String getPrompt() {
        return prompt;
    }

    public int getBatch() {
        return batch;
    }

    @Override
    public JsonNode sample() {
        if (prompt == null || prompt.isBlank()) {
            throw new TextCompletionException("llm sampler '" + getName() + "' requires a non-empty 'prompt'");
        }
        synchronized (this) {
            if (buffered.isEmpty()) {
                buffered.addAll(fetch(TextCompletionContext.require()));
            }
            return new TextNode(buffered.poll());
        }
    }

    private List<String> fetch(final TextCompletion completion) {
        if (batch == 1) {
            final String text = completion.complete(system, prompt, maxTokens, temperature);
            return List.of(clean(text));
        }
        final String batchPrompt = prompt + "\n\nProduce " + batch + " distinct variants. "
                + "Respond with ONLY a JSON array of " + batch + " strings and nothing else.";
        final String raw = completion.complete(system, batchPrompt, maxTokens * batch + 64, temperature);
        final List<String> variants = parseVariants(raw);
        if (variants.isEmpty()) {
            throw new TextCompletionException("llm sampler '" + getName() + "' received an empty batch response");
        }
        return variants;
    }

    /**
     * Parses the model's reply into variants. A JSON array (even an empty one) is authoritative; an array cut off by
     * the token limit is repaired to keep its complete elements; anything else is split on line breaks.
     */
    static List<String> parseVariants(final String raw) {
        final String body = stripCodeFence(raw);
        final int start = body.indexOf('[');
        if (start >= 0) {
            final int end = body.lastIndexOf(']');
            final String candidate = end > start ? body.substring(start, end + 1) : body.substring(start);
            final List<String> parsed = parseArray(candidate);
            if (parsed != null) {
                return parsed;
            }
            // Truncated mid-element: drop the partial tail and close the array.
            final int lastComplete = candidate.lastIndexOf("\",");
            if (lastComplete > 0) {
                final List<String> repaired = parseArray(candidate.substring(0, lastComplete + 1) + "]");
                if (repaired != null) {
                    return repaired;
                }
            }
        }
        final List<String> out = new ArrayList<>();
        for (final String line : body.split("\\R")) {
            final String value = clean(line.replaceFirst("^\\s*(?:[-*•]|\\d+[.)])\\s*", ""));
            if (!value.isEmpty()) {
                out.add(value);
            }
        }
        return out;
    }

    private static List<String> parseArray(final String json) {
        try {
            final JsonNode node = MAPPER.readTree(json);
            if (!node.isArray()) {
                return null;
            }
            final List<String> out = new ArrayList<>();
            for (final JsonNode item : node) {
                final String value = clean(item.isTextual() ? item.asText() : item.toString());
                if (!value.isEmpty()) {
                    out.add(value);
                }
            }
            return out;
        } catch (final JsonProcessingException e) {
            return null;
        }
    }

    private static String stripCodeFence(final String raw) {
        String body = raw == null ? "" : raw.strip();
        if (body.startsWith("```")) {
            final int firstBreak = body.indexOf('\n');
            body = firstBreak >= 0 ? body.substring(firstBreak + 1) : "";
            final int fenceEnd = body.lastIndexOf("```");
            if (fenceEnd >= 0) {
                body = body.substring(0, fenceEnd);
            }
        }
        return body.strip();
    }

    private static String clean(final String text) {
        if (text == null) {
            return "";
        }
        String value = text.strip();
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1).strip();
        }
        return value;
    }
}
