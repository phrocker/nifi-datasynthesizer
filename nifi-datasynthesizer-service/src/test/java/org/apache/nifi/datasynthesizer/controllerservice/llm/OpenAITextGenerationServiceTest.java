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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAITextGenerationServiceTest {

    private StubApi api;
    private TestRunner runner;
    private OpenAITextGenerationService service;

    @BeforeEach
    void setUp() throws Exception {
        api = new StubApi();
        runner = TestRunners.newTestRunner(AnthropicTextGenerationServiceTest.NoOpProcessor.class);
        service = new OpenAITextGenerationService();
        runner.addControllerService("openai", service);
        runner.setProperty(service, OpenAITextGenerationService.API_KEY, "sk-test");
        runner.setProperty(service, OpenAITextGenerationService.BASE_URL, api.baseUrl() + "/v1");
        runner.setProperty(service, OpenAITextGenerationService.MODEL, "llama3");
        runner.setProperty(service, OpenAITextGenerationService.MAX_RETRIES, "0");
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    private static String completion(final String content) {
        return "{\"id\":\"chatcmpl-1\",\"object\":\"chat.completion\",\"created\":1700000000,\"model\":\"llama3\","
                + "\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":" + content + ",\"refusal\":null},"
                + "\"finish_reason\":\"stop\",\"logprobs\":null}],"
                + "\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}";
    }

    @Test
    void sendsChatCompletionAndReturnsText() throws Exception {
        runner.enableControllerService(service);
        api.respond(200, completion("\"hi from llama\""));

        final String text = service.generate(new TextGenerationRequest("sys", "say hi", 42, 0.5));

        assertEquals("hi from llama", text);
        final StubApi.Captured request = api.last();
        assertEquals("/v1/chat/completions", request.path());
        assertTrue(request.body().contains("\"model\":\"llama3\""), request.body());
        assertTrue(request.body().contains("\"max_completion_tokens\":42"), request.body());
        assertTrue(request.body().contains("\"temperature\":0.5"), request.body());
        assertTrue(request.body().contains("\"role\":\"system\""), request.body());
        assertEquals("Bearer sk-test", request.headers().get("Authorization").get(0));
    }

    @Test
    void emptyContentBecomesIOException() throws Exception {
        runner.enableControllerService(service);
        api.respond(200, completion("\"\""));
        assertThrows(IOException.class, () -> service.generate(TextGenerationRequest.of("x", 10)));
    }

    @Test
    void serverErrorBecomesIOException() throws Exception {
        runner.enableControllerService(service);
        api.respond(500, "{\"error\":{\"message\":\"boom\",\"type\":\"server_error\"}}");
        final IOException e = assertThrows(IOException.class, () -> service.generate(TextGenerationRequest.of("x", 10)));
        assertTrue(e.getMessage().contains("request failed"), e.getMessage());
    }
}
