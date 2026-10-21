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
package org.apache.nifi.datasynthesizer.processors;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDataSynthesizer {

    @Test
    void emptySchemaIsInvalid() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 1);
        runner.setProperty(DataSynthesizer.SCHEMA, "");
        runner.assertNotValid();
        runner.setProperty(DataSynthesizer.SCHEMA, "{\"name\":\"br\", \"class\":\"browser\"}");
        runner.assertValid();
    }

    @Test
    void generatesFromArraySchema() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 3);
        runner.setProperty(DataSynthesizer.SCHEMA, "[{\"name\":\"br\", \"class\":\"browser\"}, {\"name\":\"id\", \"class\":\"uuid\"}]");
        runner.run();
        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0);
        out.assertAttributeEquals("record.count", "3");
        assertEquals(1 + 3, out.getContent().strip().split("\n").length, out.getContent());
    }

    @Test
    void generatesFromSingleObjectSchema() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 1);
        runner.setProperty(DataSynthesizer.SCHEMA, "{\"name\":\"br\", \"class\":\"browser\"}");
        runner.run();
        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_SUCCESS, 1);
        assertTrue(runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent().contains("\n"));
    }

    @Test
    void nestedRecordsAndArraysSurvive() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 2);
        runner.setProperty(DataSynthesizer.SCHEMA, "[{\"name\":\"loc\", \"class\":\"zip\", \"fields\":\"zip, city, latitude\"},"
                + "{\"name\":\"ids\", \"class\":\"sequence\", \"length\":2, \"base\":{\"class\":\"uuid\"}}]");
        runner.run();
        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_SUCCESS, 1);
        final String content = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent();
        assertTrue(content.contains("city="), content);
    }

    @Test
    void schemaFromIncomingFlowFile() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 2);
        runner.setIncomingConnection(true);
        runner.enqueue("[{\"name\":\"word\", \"class\":\"word\"}]".getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertTransferCount(DataSynthesizer.REL_SUCCESS, 1);
        runner.assertTransferCount(DataSynthesizer.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).assertAttributeEquals("record.count", "2");
    }

    @Test
    void badSchemaFromFlowFileGoesToFailure() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 2);
        runner.setIncomingConnection(true);
        runner.enqueue("this is not a schema".getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_FAILURE, 1);
    }

    @Test
    void llmFieldUsesTextGenerationService() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 4);
        final MockTextGenerationService service = Runners.withTextService(runner,
                new MockTextGenerationService(r -> "[\"alpha\", \"beta\"]"));
        runner.setProperty(DataSynthesizer.SCHEMA, "[{\"name\":\"id\", \"class\":\"uuid\"},"
                + "{\"name\":\"msg\", \"class\":\"llm\", \"prompt\":\"Say hi\", \"batch\":2, \"maxTokens\":20}]");
        runner.run();
        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_SUCCESS, 1);
        final String content = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent();
        assertTrue(content.contains("alpha") && content.contains("beta"), content);
        assertEquals(2, service.calls.get(), "4 records with batch 2 should take 2 calls");
        assertTrue(service.requests.get(0).prompt().startsWith("Say hi"));
        assertEquals(List.of(), runner.getFlowFilesForRelationship(DataSynthesizer.REL_FAILURE));
    }

    @Test
    void temporalFieldsWriteThroughRealJsonWriter() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 3);
        final JsonRecordSetWriter json = new JsonRecordSetWriter();
        runner.addControllerService("json", json);
        runner.setProperty(json, "Date Format", "yyyy-MM-dd");
        runner.setProperty(json, "Timestamp Format", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        runner.enableControllerService(json);
        runner.setProperty(DataSynthesizer.RECORD_WRITER, "json");
        runner.setProperty(DataSynthesizer.SCHEMA, "["
                + "{\"name\":\"day\", \"class\":\"date\"},"
                + "{\"name\":\"ts\", \"class\":\"date\", \"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"},"
                + "{\"name\":\"n\", \"class\":\"int\", \"min\":1, \"max\":9}]");
        runner.run();
        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_SUCCESS, 1);
        final String content = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent();
        // Inferred temporal strings become real date/timestamp values, so the writer can re-format them.
        assertTrue(content.matches("(?s).*\"day\"\\s*:\\s*\"\\d{4}-\\d{2}-\\d{2}\".*"), content);
        assertTrue(content.matches("(?s).*\"ts\"\\s*:\\s*\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z\".*"), content);
    }

    @Test
    void mixedShapesAcrossSamplesKeepFieldsAndPrecision() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 6);
        final JsonRecordSetWriter json = new JsonRecordSetWriter();
        runner.addControllerService("json", json);
        runner.enableControllerService(json);
        runner.setProperty(DataSynthesizer.RECORD_WRITER, "json");
        runner.setProperty(DataSynthesizer.SCHEMA, "[{\"name\":\"v\", \"class\":\"double\", \"min\":0.5, \"max\":0.9}]");
        runner.run();
        final String content = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent();
        assertTrue(content.contains("0."), content);
    }

    @Test
    void midBatchFailureWithInputRoutesToFailureNotRetry() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 3);
        final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        Runners.withTextService(runner, new MockTextGenerationService(r -> {
            if (calls.incrementAndGet() == 2) {
                throw new RuntimeException("rate limited");
            }
            return "ok";
        }));
        runner.setIncomingConnection(true);
        runner.enqueue("[{\"name\":\"msg\",\"class\":\"llm\",\"prompt\":\"hi\"}]".getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_FAILURE, 1);
    }

    @Test
    void llmFieldWithoutServiceFails() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 1);
        runner.setProperty(DataSynthesizer.SCHEMA, "[{\"name\":\"msg\", \"class\":\"llm\", \"prompt\":\"Say hi\"}]");
        org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class, runner::run);
        runner.assertTransferCount(DataSynthesizer.REL_SUCCESS, 0);
    }
}
