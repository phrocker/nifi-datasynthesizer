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

import com.fasterxml.jackson.databind.JsonNode;
import com.mapr.synth.samplers.SchemaSampler;
import org.apache.nifi.datasynthesizer.llm.TextCompletion;
import org.apache.nifi.datasynthesizer.llm.TextCompletionContext;
import org.apache.nifi.datasynthesizer.llm.TextCompletionException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LlmTextSamplerTest {

    @Test
    void singleCompletionPerSample() throws Exception {
        final AtomicInteger calls = new AtomicInteger();
        final TextCompletion fake = (system, prompt, maxTokens, temperature) -> {
            assertEquals(40, maxTokens);
            assertTrue(prompt.startsWith("Write a greeting"));
            return "hello #" + calls.incrementAndGet();
        };
        final SchemaSampler sampler = new SchemaSampler(
                "[{\"name\":\"msg\",\"class\":\"llm\",\"prompt\":\"Write a greeting\",\"maxTokens\":40}]");
        try (TextCompletionContext.Scope ignored = TextCompletionContext.use(fake)) {
            assertEquals("hello #1", sampler.sample().get("msg").asText());
            assertEquals("hello #2", sampler.sample().get("msg").asText());
        }
        assertEquals(2, calls.get());
    }

    @Test
    void batchingReducesCalls() throws Exception {
        final AtomicInteger calls = new AtomicInteger();
        final TextCompletion fake = (system, prompt, maxTokens, temperature) -> {
            calls.incrementAndGet();
            assertTrue(prompt.contains("Produce 3 distinct variants"));
            return "```json\n[\"a\", \"b\", \"c\"]\n```";
        };
        final SchemaSampler sampler = new SchemaSampler(
                "[{\"name\":\"msg\",\"class\":\"llm\",\"prompt\":\"Say something\",\"batch\":3}]");
        final List<String> seen = new ArrayList<>();
        try (TextCompletionContext.Scope ignored = TextCompletionContext.use(fake)) {
            for (int i = 0; i < 5; i++) {
                final JsonNode node = sampler.sample();
                seen.add(node.get("msg").asText());
            }
        }
        assertEquals(List.of("a", "b", "c", "a", "b"), seen);
        assertEquals(2, calls.get());
    }

    @Test
    void fallsBackToLinesWhenNotJson() {
        final List<String> variants = LlmTextSampler.parseVariants("1. first line\n- \"second line\"\n\nthird");
        assertEquals(List.of("first line", "second line", "third"), variants);
    }

    @Test
    void emptyArrayIsEmptyNotLiteral() {
        assertEquals(List.of(), LlmTextSampler.parseVariants("[]"));
        assertEquals(List.of(), LlmTextSampler.parseVariants("[\"\"]"));
        assertEquals(List.of(), LlmTextSampler.parseVariants("```json\n[]\n```"));
    }

    @Test
    void truncatedArrayKeepsCompleteElements() {
        assertEquals(List.of("first one", "second"), LlmTextSampler.parseVariants("[\"first one\", \"second\", \"thir"));
        assertEquals(List.of("only"), LlmTextSampler.parseVariants("[\"only\", \"cut"));
    }

    @Test
    void emptyBatchFailsInsteadOfFillingRecords() throws Exception {
        final SchemaSampler sampler = new SchemaSampler(
                "[{\"name\":\"msg\",\"class\":\"llm\",\"prompt\":\"x\",\"batch\":3}]");
        try (TextCompletionContext.Scope ignored = TextCompletionContext.use((s, p, m, t) -> "[]")) {
            final TextCompletionException e = assertThrows(TextCompletionException.class, sampler::sample);
            assertTrue(e.getMessage().contains("empty batch"));
        }
    }

    @Test
    void failsClearlyWithoutProvider() throws Exception {
        final SchemaSampler sampler = new SchemaSampler("[{\"name\":\"msg\",\"class\":\"llm\",\"prompt\":\"x\"}]");
        final TextCompletionException e = assertThrows(TextCompletionException.class, sampler::sample);
        assertTrue(e.getMessage().contains("Text Generation Service"));
    }

    @Test
    void failsWithoutPrompt() throws Exception {
        final SchemaSampler sampler = new SchemaSampler("[{\"name\":\"msg\",\"class\":\"llm\"}]");
        try (TextCompletionContext.Scope ignored = TextCompletionContext.use((s, p, m, t) -> "x")) {
            assertThrows(TextCompletionException.class, sampler::sample);
        }
    }

    @Test
    void scopeRestoresPreviousProvider() {
        final TextCompletion outer = (s, p, m, t) -> "outer";
        final TextCompletion inner = (s, p, m, t) -> "inner";
        try (TextCompletionContext.Scope a = TextCompletionContext.use(outer)) {
            try (TextCompletionContext.Scope b = TextCompletionContext.use(inner)) {
                assertEquals("inner", TextCompletionContext.require().complete(null, "", 1, null));
            }
            assertEquals("outer", TextCompletionContext.require().complete(null, "", 1, null));
        }
        assertTrue(TextCompletionContext.current().isEmpty());
        assertTrue(Set.of(1).contains(1));
    }
}
