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

import org.apache.nifi.datasynthesizer.processors.chaos.ChaosProfile;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestChaos {

    private static final Pattern ID = Pattern.compile("\"chaos_id\"\\s*:\\s*\"([0-9a-f-]{36})\"");

    private static TestRunner runner(final int count, final String schema) throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, count);
        final JsonRecordSetWriter json = new JsonRecordSetWriter();
        runner.addControllerService("json", json);
        runner.enableControllerService(json);
        runner.setProperty(DataSynthesizer.RECORD_WRITER, "json");
        runner.setProperty(DataSynthesizer.SCHEMA, schema);
        return runner;
    }

    private static List<String> faultSequence(final String manifest) {
        final List<String> faults = new java.util.ArrayList<>();
        final Matcher m = Pattern.compile("\"fault\":\"(\\w+)\"").matcher(manifest);
        while (m.find()) {
            faults.add(m.group(1));
        }
        return faults;
    }

    private static Set<String> ids(final String content) {
        final Set<String> ids = new HashSet<>();
        final Matcher m = ID.matcher(content);
        while (m.find()) {
            ids.add(m.group(1));
        }
        return ids;
    }

    @Test
    void disabledByDefaultNoTagNoManifest() throws Exception {
        final TestRunner runner = runner(3, "[{\"name\":\"id\",\"class\":\"uuid\"}]");
        runner.run();
        runner.assertTransferCount(DataSynthesizer.REL_SUCCESS, 1);
        runner.assertTransferCount(DataSynthesizer.REL_CHAOS, 0);
        assertFalse(runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent().contains("chaos_id"));
    }

    @Test
    void fieldFaultsAreTaggedAndManifested() throws Exception {
        final TestRunner runner = runner(10, "[{\"name\":\"id\",\"class\":\"uuid\"},"
                + "{\"name\":\"email\",\"class\":\"chaos\",\"rate\":1,\"faults\":[\"null\"],\"value\":{\"class\":\"email\"}}]");
        runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
        runner.run();
        runner.assertTransferCount(DataSynthesizer.REL_SUCCESS, 1);
        runner.assertTransferCount(DataSynthesizer.REL_CHAOS, 1);

        final String records = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent();
        final MockFlowFile manifestFlowFile = runner.getFlowFilesForRelationship(DataSynthesizer.REL_CHAOS).get(0);
        final String manifest = manifestFlowFile.getContent();
        manifestFlowFile.assertAttributeEquals("record.count", "10");
        assertEquals(10, ids(records).size(), records);
        assertEquals(ids(records), ids(manifest), "every tagged record has a manifest row");
        assertTrue(manifest.contains("\"fault\":\"null\"") && manifest.contains("\"field\":\"email\""), manifest);
        assertTrue(records.contains("\"email\":null"), records);
    }

    @Test
    void cleanRecordsCarryNullTag() throws Exception {
        final TestRunner runner = runner(4, "[{\"name\":\"id\",\"class\":\"uuid\"}]");
        runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
        runner.run();
        runner.assertTransferCount(DataSynthesizer.REL_CHAOS, 0);
        final String records = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent();
        assertEquals(4, records.split("\"chaos_id\":null", -1).length - 1, records);
    }

    @Test
    void separateRoutingSendsTamperedRecordsToInvalid() throws Exception {
        final TestRunner runner = runner(40, "[{\"name\":\"id\",\"class\":\"uuid\"},"
                + "{\"name\":\"email\",\"class\":\"chaos\",\"rate\":0.5,\"seed\":3,\"faults\":[\"null\"],\"value\":{\"class\":\"email\"}}]");
        runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
        runner.setProperty(ChaosProfile.CHAOS_ROUTING, "separate");
        runner.run();
        runner.assertTransferCount(DataSynthesizer.REL_SUCCESS, 1);
        runner.assertTransferCount(DataSynthesizer.REL_INVALID, 1);
        runner.assertTransferCount(DataSynthesizer.REL_CHAOS, 1);
        final MockFlowFile good = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0);
        final MockFlowFile bad = runner.getFlowFilesForRelationship(DataSynthesizer.REL_INVALID).get(0);
        assertEquals(0, ids(good.getContent()).size(), "clean output must not contain tampered records");
        assertFalse(good.getContent().contains("\"email\":null"), good.getContent());
        assertEquals(Integer.parseInt(bad.getAttribute("record.count")), ids(bad.getContent()).size(), bad.getContent());
        assertEquals(ids(bad.getContent()), ids(runner.getFlowFilesForRelationship(DataSynthesizer.REL_CHAOS).get(0).getContent()));
        assertEquals(40, Integer.parseInt(good.getAttribute("record.count")) + Integer.parseInt(bad.getAttribute("record.count")));
    }

    @Test
    void driftAddsUnrelatedFieldThatSurvivesTheWriter() throws Exception {
        final TestRunner runner = runner(60, "[{\"name\":\"id\",\"class\":\"uuid\"},{\"name\":\"n\",\"class\":\"int\",\"min\":1,\"max\":9}]");
        runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
        runner.setProperty(ChaosProfile.CHAOS_DRIFT_RATE, "0.5");
        runner.setProperty(ChaosProfile.CHAOS_SEED, "11");
        runner.run();
        final String records = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent();
        final String manifest = runner.getFlowFilesForRelationship(DataSynthesizer.REL_CHAOS).get(0).getContent();
        assertTrue(records.contains("\"chaos_unexpected\":\"unexpected-"), records);
        assertTrue(manifest.contains("\"field\":\"chaos_unexpected\""), manifest);
        assertTrue(manifest.contains("\"field\":\"n\"") || manifest.contains("\"field\":\"id\""), "removal drift expected too: " + manifest);
    }

    @Test
    void duplicateDropAndDriftAreReproducibleWithSeed() throws Exception {
        final String schema = "[{\"name\":\"id\",\"class\":\"uuid\",\"seed\":5},{\"name\":\"n\",\"class\":\"int\",\"min\":1,\"max\":9}]";
        String firstRecords = null;
        String firstManifest = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            final TestRunner runner = runner(50, schema);
            runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
            runner.setProperty(ChaosProfile.CHAOS_DUPLICATE_RATE, "0.2");
            runner.setProperty(ChaosProfile.CHAOS_DROP_RATE, "0.2");
            runner.setProperty(ChaosProfile.CHAOS_DRIFT_RATE, "0.2");
            runner.setProperty(ChaosProfile.CHAOS_SEED, "99");
            runner.run();
            final String records = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent();
            final String manifest = runner.getFlowFilesForRelationship(DataSynthesizer.REL_CHAOS).get(0).getContent();
            assertTrue(manifest.contains("\"fault\":\"duplicate\""), manifest);
            assertTrue(manifest.contains("\"fault\":\"drop\""), manifest);
            assertTrue(manifest.contains("\"fault\":\"drift\""), manifest);
            // dropped ids are in the manifest but not in the output
            for (final String line : manifest.split("\\},\\{")) {
                if (line.contains("\"fault\":\"drop\"")) {
                    final Matcher m = ID.matcher(line);
                    assertTrue(m.find());
                    assertFalse(records.contains(m.group(1)), "dropped record leaked: " + m.group(1));
                }
            }
            final int written = Integer.parseInt(runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getAttribute("record.count"));
            assertTrue(written != 50, "duplicates and drops should change the count: " + written);
            if (attempt == 0) {
                firstRecords = records;
                firstManifest = manifest;
            } else {
                assertEquals(faultSequence(firstManifest), faultSequence(manifest),
                        "same seed must inject the same faults in the same order");
            }
        }
        assertTrue(firstRecords != null && !List.of().contains(firstRecords));
    }
}
