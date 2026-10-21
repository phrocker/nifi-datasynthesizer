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
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.datasynthesizer.chaos.ChaosLedger;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
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
    void droppingEveryRecordStillEmitsOutputAndManifest() throws Exception {
        final TestRunner runner = runner(5, "[{\"name\":\"id\",\"class\":\"uuid\"}]");
        runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
        runner.setProperty(ChaosProfile.CHAOS_DROP_RATE, "1");
        runner.run();
        runner.assertTransferCount(DataSynthesizer.REL_SUCCESS, 1);
        runner.assertTransferCount(DataSynthesizer.REL_CHAOS, 1);
        runner.assertTransferCount(DataSynthesizer.REL_FAILURE, 0);
        runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).assertAttributeEquals("record.count", "0");
        final MockFlowFile manifest = runner.getFlowFilesForRelationship(DataSynthesizer.REL_CHAOS).get(0);
        manifest.assertAttributeEquals("record.count", "5");
        assertEquals(5, manifest.getContent().split("\"fault\":\"drop\"", -1).length - 1);
    }

    @Test
    void manifestDoesNotDependOnTheConfiguredRecordWriter() throws Exception {
        // MockRecordWriter prints 'header' and comma separated values; the manifest must still be JSON lines.
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 3);
        runner.setProperty(DataSynthesizer.SCHEMA, "[{\"name\":\"v\",\"class\":\"chaos\",\"rate\":1,\"faults\":[\"null\"],\"value\":{\"class\":\"uuid\"}}]");
        runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
        runner.run();
        final MockFlowFile manifest = runner.getFlowFilesForRelationship(DataSynthesizer.REL_CHAOS).get(0);
        manifest.assertAttributeEquals("mime.type", "application/x-ndjson");
        manifest.assertAttributeEquals("chaos.manifest", "true");
        final String[] lines = manifest.getContent().strip().split("\n");
        assertEquals(3, lines.length, manifest.getContent());
        for (final String line : lines) {
            assertTrue(line.startsWith("{\"chaos_id\":\"") && line.endsWith("}"), line);
        }
    }

    @Test
    void typeFaultAfterInferenceWindowStillSerializesWithAvro() throws Exception {
        // Find a seed where the first 10 draws are clean and a later one swaps the type.
        String schema = null;
        for (long seed = 1; seed < 500 && schema == null; seed++) {
            final String candidate = "[{\"name\":\"id\",\"class\":\"uuid\"},{\"name\":\"amount\",\"class\":\"chaos\",\"rate\":0.3,"
                    + "\"seed\":" + seed + ",\"faults\":[\"type\"],\"value\":{\"class\":\"int\",\"min\":1,\"max\":9}}]";
            final com.mapr.synth.samplers.SchemaSampler probe = new com.mapr.synth.samplers.SchemaSampler(candidate);
            boolean cleanHead = true;
            for (int i = 0; i < 10; i++) {
                ChaosLedger.begin();
                probe.sample();
                cleanHead &= ChaosLedger.drain().isEmpty();
            }
            boolean laterFault = false;
            for (int i = 0; i < 20; i++) {
                ChaosLedger.begin();
                probe.sample();
                laterFault |= !ChaosLedger.drain().isEmpty();
            }
            if (cleanHead && laterFault) {
                schema = candidate;
            }
        }
        assertTrue(schema != null, "no seed found");
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 30);
        final AvroRecordSetWriter avro = new AvroRecordSetWriter();
        runner.addControllerService("avro", avro);
        runner.enableControllerService(avro);
        runner.setProperty(DataSynthesizer.RECORD_WRITER, "avro");
        runner.setProperty(DataSynthesizer.SCHEMA, schema);
        runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
        runner.run();
        runner.assertTransferCount(DataSynthesizer.REL_SUCCESS, 1);
        runner.assertTransferCount(DataSynthesizer.REL_CHAOS, 1);
        runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).assertAttributeEquals("record.count", "30");
    }

    @Test
    void driftRemovalIsAbsentWhenWriterSuppressesMissingValues() throws Exception {
        final TestRunner runner = Runners.withWriter(DataSynthesizer.class, 60);
        final JsonRecordSetWriter json = new JsonRecordSetWriter();
        runner.addControllerService("json", json);
        runner.setProperty(json, JsonRecordSetWriter.SUPPRESS_NULLS, JsonRecordSetWriter.SUPPRESS_MISSING.getValue());
        runner.enableControllerService(json);
        runner.setProperty(DataSynthesizer.RECORD_WRITER, "json");
        runner.setProperty(DataSynthesizer.SCHEMA, "[{\"name\":\"id\",\"class\":\"uuid\"},{\"name\":\"n\",\"class\":\"int\",\"min\":1,\"max\":9}]");
        runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
        runner.setProperty(ChaosProfile.CHAOS_DRIFT_RATE, "0.5");
        runner.setProperty(ChaosProfile.CHAOS_SEED, "11");
        runner.run();
        final String records = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0).getContent();
        final String manifest = runner.getFlowFilesForRelationship(DataSynthesizer.REL_CHAOS).get(0).getContent();
        int checked = 0;
        for (final String line : manifest.split("\n")) {
            if (line.contains("\"fault\":\"drift\"") && !line.contains("\"field\":\"chaos_unexpected\"")) {
                final Matcher id = ID.matcher(line);
                final Matcher field = Pattern.compile("\"field\":\"(\\w+)\"").matcher(line);
                assertTrue(id.find() && field.find(), line);
                for (final String record : records.split("\\},\\{")) {
                    if (record.contains(id.group(1))) {
                        assertFalse(record.contains("\"" + field.group(1) + "\":"), "removed field still written: " + record);
                        checked++;
                    }
                }
            }
        }
        assertTrue(checked > 0, "expected at least one removal drift: " + manifest);
    }

    @Test
    void correlatorTagsAndRoutesTamperedCorrelations() throws Exception {
        final TestRunner runner = Runners.withWriter(DataCorrelator.class, 2);
        final JsonRecordSetWriter json = new JsonRecordSetWriter();
        runner.addControllerService("json", json);
        runner.enableControllerService(json);
        runner.setProperty(DataSynthesizer.RECORD_WRITER, "json");
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("name", RecordFieldType.STRING);
        reader.addRecord("Ann");
        reader.addRecord("Bob");
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(DataCorrelator.RECORD_READER, "reader");
        runner.setProperty(DataCorrelator.SCHEMA, "[{\"name\":\"device\",\"class\":\"map\",\"value\":["
                + "{\"name\":\"ip\",\"class\":\"chaos\",\"rate\":1,\"faults\":[\"null\"],\"value\":{\"class\":\"ipv4\"}}]}]");
        runner.setProperty(DataCorrelator.SCHEMA_KEY, "device");
        runner.setProperty(ChaosProfile.CHAOS_ENABLED, "true");
        runner.setProperty(ChaosProfile.CHAOS_ROUTING, "separate");
        runner.setIncomingConnection(true);
        runner.enqueue("ignored");
        runner.run();
        runner.assertTransferCount(DataCorrelator.REL_SUCCESS, 1);
        runner.assertTransferCount(DataCorrelator.REL_INVALID, 1);
        runner.assertTransferCount(DataCorrelator.REL_CORRELATION, 1);
        runner.assertTransferCount(DataCorrelator.REL_CHAOS, 1);
        runner.assertTransferCount(DataCorrelator.REL_FAILURE, 0);
        runner.getFlowFilesForRelationship(DataCorrelator.REL_SUCCESS).get(0).assertAttributeEquals("record.count", "0");
        final MockFlowFile invalid = runner.getFlowFilesForRelationship(DataCorrelator.REL_INVALID).get(0);
        invalid.assertAttributeEquals("record.count", "4");
        assertEquals(4, ids(invalid.getContent()).size(), invalid.getContent());
        assertTrue(invalid.getContent().contains("Ann") && invalid.getContent().contains("\"ip\":null"), invalid.getContent());
        final String manifest = runner.getFlowFilesForRelationship(DataCorrelator.REL_CHAOS).get(0).getContent();
        assertEquals(ids(invalid.getContent()), ids(manifest));
        assertEquals(4, ids(runner.getFlowFilesForRelationship(DataCorrelator.REL_CORRELATION).get(0).getContent()).size());
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
