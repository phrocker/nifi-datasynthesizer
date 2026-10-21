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

import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDataCorrelator {

    private TestRunner runner(final int perRecord, final boolean selectObject) throws Exception {
        final TestRunner runner = Runners.withWriter(DataCorrelator.class, perRecord);
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("name", RecordFieldType.STRING);
        reader.addSchemaField("age", RecordFieldType.INT);
        reader.addRecord("Ann", 31);
        reader.addRecord("Bob", 42);
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(DataCorrelator.RECORD_READER, "reader");
        runner.setProperty(DataCorrelator.SCHEMA, "[{\"name\":\"device\", \"class\":\"map\", \"value\":["
                + "{\"name\":\"id\", \"class\":\"uuid\"}, {\"name\":\"ip\", \"class\":\"ipv4\"}]}]");
        runner.setProperty(DataCorrelator.SCHEMA_KEY, "device");
        runner.setProperty(DataCorrelator.SELECT_OBJECT, String.valueOf(selectObject));
        runner.enqueue("ignored by mock parser");
        return runner;
    }

    @Test
    void enrichesEveryRecordAndEmitsCorrelations() throws Exception {
        final TestRunner runner = runner(2, true);
        runner.run();
        runner.assertTransferCount(DataCorrelator.REL_SUCCESS, 1);
        runner.assertTransferCount(DataCorrelator.REL_CORRELATION, 1);
        runner.assertTransferCount(DataCorrelator.REL_ORIGINAL, 1);
        runner.assertTransferCount(DataCorrelator.REL_FAILURE, 0);

        final MockFlowFile enriched = runner.getFlowFilesForRelationship(DataCorrelator.REL_SUCCESS).get(0);
        enriched.assertAttributeEquals("record.count", "4");
        final String content = enriched.getContent();
        assertTrue(content.contains("Ann") && content.contains("Bob"), content);
        assertTrue(content.contains("ip="), content);

        final MockFlowFile correlation = runner.getFlowFilesForRelationship(DataCorrelator.REL_CORRELATION).get(0);
        correlation.assertAttributeEquals("record.count", "4");
        assertEquals(1 + 4, correlation.getContent().strip().split("\n").length, correlation.getContent());
    }

    @Test
    void wholeObjectWhenNotSelecting() throws Exception {
        final TestRunner runner = runner(1, false);
        runner.run();
        runner.assertTransferCount(DataCorrelator.REL_SUCCESS, 1);
        final String content = runner.getFlowFilesForRelationship(DataCorrelator.REL_SUCCESS).get(0).getContent();
        assertTrue(content.contains("device="), content);
    }
}
