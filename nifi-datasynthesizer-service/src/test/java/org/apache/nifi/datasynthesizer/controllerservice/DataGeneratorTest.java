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
import org.apache.nifi.datasynthesizer.controllerservice.llm.AnthropicTextGenerationServiceTest;
import org.apache.nifi.datasynthesizer.services.TextGenerationService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataGeneratorTest {

    @Test
    void servesInlineAndFileSchemas(@TempDir final Path dir) throws Exception {
        final Path file = dir.resolve("phone.json");
        Files.writeString(file, "[{\"name\":\"id\",\"class\":\"uuid\"}]");

        final TestRunner runner = TestRunners.newTestRunner(AnthropicTextGenerationServiceTest.NoOpProcessor.class);
        final DataGenerator generator = new DataGenerator();
        runner.addControllerService("gen", generator);
        runner.setProperty(generator, "browser", "{\"name\":\"br\",\"class\":\"browser\"}");
        runner.setProperty(generator, "fromFile", file.toString());
        runner.assertValid(generator);
        runner.enableControllerService(generator);

        assertTrue(generator.hasSchema("browser"));
        assertTrue(generator.hasSchema("fromFile"));
        assertTrue(!generator.hasSchema("missing"));
        assertTrue(generator.sampleSchema("browser").orElseThrow().has("br"));
        assertEquals(36, generator.sampleSchema("fromFile").orElseThrow().get("id").asText().length());
        assertEquals(Optional.empty(), generator.sampleSchema("missing"));
    }

    @Test
    void invalidSchemaFailsValidation() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(AnthropicTextGenerationServiceTest.NoOpProcessor.class);
        final DataGenerator generator = new DataGenerator();
        runner.addControllerService("gen", generator);
        runner.setProperty(generator, "bad", "/definitely/not/a/file.json");
        runner.assertNotValid(generator);
    }

    @Test
    void llmSchemaUsesConfiguredTextService() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(AnthropicTextGenerationServiceTest.NoOpProcessor.class);
        final TextGenerationService text = new StubTextService();
        runner.addControllerService("text", text);
        runner.enableControllerService(text);
        final DataGenerator generator = new DataGenerator();
        runner.addControllerService("gen", generator);
        runner.setProperty(generator, DataGenerator.TEXT_GENERATION_SERVICE, "text");
        runner.setProperty(generator, "msg", "[{\"name\":\"m\",\"class\":\"llm\",\"prompt\":\"hi\"}]");
        runner.enableControllerService(generator);

        final JsonNode node = generator.sampleSchema("msg").orElseThrow();
        assertEquals("stubbed", node.get("m").asText());
    }

    public static class StubTextService extends org.apache.nifi.controller.AbstractControllerService implements TextGenerationService {
        @Override
        public String generate(final org.apache.nifi.datasynthesizer.services.TextGenerationRequest request) {
            return "stubbed";
        }
    }
}
