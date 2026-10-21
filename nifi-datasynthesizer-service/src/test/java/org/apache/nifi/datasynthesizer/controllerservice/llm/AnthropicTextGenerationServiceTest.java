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

import org.apache.nifi.datasynthesizer.services.TextGenerationRequest;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AnthropicTextGenerationServiceTest {

    public static class NoOpProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }
    }

    private StubApi api;
    private TestRunner runner;
    private AnthropicTextGenerationService service;

    @BeforeEach
    void setUp() throws Exception {
        api = new StubApi();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new AnthropicTextGenerationService();
        runner.addControllerService("anthropic", service);
        runner.setProperty(service, AnthropicTextGenerationService.API_KEY, "sk-ant-test");
        runner.setProperty(service, AnthropicTextGenerationService.BASE_URL, api.baseUrl());
        runner.setProperty(service, AnthropicTextGenerationService.MAX_RETRIES, "0");
        runner.setProperty(service, AnthropicTextGenerationService.MODEL, "claude-opus-5-5");
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    private static String message(final String stopReason, final String text) {
        return "{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-opus-5-5\","
                + "\"content\":[{\"type\":\"text\",\"text\":\"" + text + "\"}],"
                + "\"stop_reason\":\"" + stopReason + "\",\"stop_sequence\":null,"
                + "\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}";
    }

    @Test
    void sendsMessagesRequestAndReturnsText() throws Exception {
        runner.enableControllerService(service);
        api.respond(200, message("end_turn", "hello there"));

        final String text = service.generate(new TextGenerationRequest("be brief", "say hi", 50, 0.7));

        assertEquals("hello there", text);
        final StubApi.Captured request = api.last();
        assertEquals("/v1/messages", request.path());
        assertTrue(request.body().contains("\"model\":\"claude-opus-5-5\""), request.body());
        assertTrue(request.body().contains("\"max_tokens\":50"), request.body());
        assertTrue(request.body().contains("\"system\":\"be brief\""), request.body());
        assertTrue(request.body().contains("\"effort\":\"low\""), request.body());
        assertTrue(request.body().contains("\"fallbacks\":\"default\""), request.body());
        assertTrue(!request.body().contains("temperature"), "temperature must not be forwarded: " + request.body());
        assertEquals("sk-ant-test", request.headers().get("X-api-key").get(0));
        assertTrue(String.join(",", request.headers().get("Anthropic-beta")).contains("server-side-fallback-2026-07-01"));
    }

    @Test
    void fallbacksCanBeDisabled() throws Exception {
        runner.setProperty(service, AnthropicTextGenerationService.REFUSAL_FALLBACKS, "false");
        runner.setProperty(service, AnthropicTextGenerationService.EFFORT, "high");
        runner.enableControllerService(service);
        api.respond(200, message("end_turn", "ok"));

        service.generate(TextGenerationRequest.of("x", 10));

        assertTrue(!api.last().body().contains("fallbacks"), api.last().body());
        assertTrue(api.last().body().contains("\"effort\":\"high\""), api.last().body());
    }

    @Test
    void refusalBecomesIOException() throws Exception {
        runner.enableControllerService(service);
        api.respond(200, "{\"id\":\"msg_2\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-opus-5-5\","
                + "\"content\":[],\"stop_reason\":\"refusal\",\"stop_sequence\":null,"
                + "\"stop_details\":{\"type\":\"refusal\",\"category\":\"cyber\",\"explanation\":\"nope\"},"
                + "\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}");

        final IOException e = assertThrows(IOException.class, () -> service.generate(TextGenerationRequest.of("x", 10)));
        assertTrue(e.getMessage().contains("declined"), e.getMessage());
        assertTrue(e.getMessage().contains("cyber"), e.getMessage());
    }

    @Test
    void httpErrorBecomesIOException() throws Exception {
        runner.enableControllerService(service);
        api.respond(401, "{\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"bad key\"}}");

        final IOException e = assertThrows(IOException.class, () -> service.generate(TextGenerationRequest.of("x", 10)));
        assertTrue(e.getMessage().contains("Anthropic request failed"), e.getMessage());
    }

    @Test
    void requiresApiKey() {
        runner.removeProperty(service, AnthropicTextGenerationService.API_KEY);
        runner.assertNotValid(service);
    }
}
