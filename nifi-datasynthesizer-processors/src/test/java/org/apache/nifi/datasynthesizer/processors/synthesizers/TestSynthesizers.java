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
package org.apache.nifi.datasynthesizer.processors.synthesizers;

import org.apache.nifi.datasynthesizer.processors.DataSynthesizerBase;
import org.apache.nifi.datasynthesizer.processors.MockTextGenerationService;
import org.apache.nifi.datasynthesizer.processors.Runners;
import org.apache.nifi.datasynthesizer.processors.synthesizers.cars.CommuterData;
import org.apache.nifi.datasynthesizer.processors.synthesizers.finance.Transactions;
import org.apache.nifi.datasynthesizer.processors.synthesizers.iot.IotData;
import org.apache.nifi.datasynthesizer.processors.synthesizers.telephony.PhoneNumber;
import org.apache.nifi.datasynthesizer.processors.synthesizers.telephony.TextMessage;
import org.apache.nifi.datasynthesizer.processors.synthesizers.text.TextGenerator;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSynthesizers {

    private static String success(final TestRunner runner) {
        runner.run();
        runner.assertAllFlowFilesTransferred(DataSynthesizerBase.REL_SUCCESS, 1);
        return runner.getFlowFilesForRelationship(DataSynthesizerBase.REL_SUCCESS).get(0).getContent();
    }

    @Test
    void commuterRandomHome() throws Exception {
        final String content = success(Runners.withWriter(CommuterData.class, 1));
        assertTrue(content.contains("home="), content);
    }

    @Test
    void commuterPinnedToZip() throws Exception {
        final TestRunner runner = Runners.withWriter(CommuterData.class, 1);
        runner.setProperty(CommuterData.HOME_LOCATION, "1000");
        runner.assertNotValid();
        runner.setProperty(CommuterData.HOME_LOCATION, "10001");
        runner.assertValid();
        assertTrue(success(runner).contains("city=NEW YORK"));
    }

    @Test
    void iot() throws Exception {
        final String content = success(Runners.withWriter(IotData.class, 2));
        final String[] lines = content.strip().split("\n");
        assertEquals(3, lines.length, content);
        // uuid, ip, latitude, longitude, timestamp, temperature
        assertTrue(lines[1].matches("[0-9a-f-]{36},\\d+\\.\\d+\\.\\d+\\.\\d+,-?\\d+\\.\\d+,-?\\d+\\.\\d+,\\d{4}-\\d{2}-\\d{2} .*,-?\\d+.*"), content);
    }

    @Test
    void phoneNumberWithAreaCode() throws Exception {
        final TestRunner runner = Runners.withWriter(PhoneNumber.class, 5);
        runner.setProperty(PhoneNumber.AREA_CODE, "497");
        final String content = success(runner);
        final Matcher m = Pattern.compile("497-\\d{3}-\\d{4}").matcher(content);
        int count = 0;
        while (m.find()) {
            count++;
        }
        assertEquals(5, count, content);
    }

    @Test
    void textMessageMarkov() throws Exception {
        final TestRunner runner = Runners.withWriter(TextMessage.class, 2);
        runner.setProperty(TextMessage.SOURCE_AREA_CODE, "212");
        runner.setProperty(TextMessage.WORD_LENGTH, "8");
        final String content = success(runner);
        assertTrue(content.contains("212-"), content);
        assertTrue(content.contains("T"), content);
    }

    @Test
    void textMessageLlm() throws Exception {
        final TestRunner runner = Runners.withWriter(TextMessage.class, 3);
        final MockTextGenerationService service = Runners.withTextService(runner,
                new MockTextGenerationService(r -> "[\"running late, order without me\", \"did you feed the cat\", \"yes!!\"]"));
        runner.setProperty(TextMessage.LLM_BATCH, "3");
        final String content = success(runner);
        assertTrue(content.contains("feed the cat"), content);
        assertEquals(1, service.calls.get());
        assertTrue(service.requests.get(0).prompt().contains("SMS"));
    }

    @Test
    void textGeneratorMarkov() throws Exception {
        final TestRunner runner = Runners.withWriter(TextGenerator.class, 1);
        runner.setProperty(TextGenerator.WORD_LENGTH, "12");
        runner.setProperty(TextGenerator.CORPUS, "us_const");
        final String content = success(runner);
        assertTrue(content.strip().split("\n")[1].split(" ").length >= 12, content);
    }

    @Test
    void textGeneratorLlm() throws Exception {
        final TestRunner runner = Runners.withWriter(TextGenerator.class, 1);
        Runners.withTextService(runner, new MockTextGenerationService(r -> "Once upon a time."));
        runner.setProperty(TextGenerator.LLM_BATCH, "1");
        assertTrue(success(runner).contains("Once upon a time."));
    }

    @Test
    void transactionsStandalone() throws Exception {
        final TestRunner runner = Runners.withWriter(Transactions.class, 2);
        runner.setIncomingConnection(false);
        runner.setProperty(Transactions.TERMINALS_TO_GENERATE, "5");
        runner.setProperty(Transactions.MAX_TRANSACTIONS, "3");
        runner.setProperty(Transactions.FIXED_TRANSACTIONS, "true");
        final String content = success(runner);
        assertTrue(content.contains("terminal_city="), content);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DataSynthesizerBase.REL_SUCCESS).get(0);
        out.assertAttributeEquals("record.count", "2");
    }

    @Test
    void transactionsEnrichInput() throws Exception {
        final TestRunner runner = Runners.withWriter(Transactions.class, 1);
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("name", RecordFieldType.STRING);
        reader.addRecord("Ann");
        reader.addRecord("Bob");
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(Transactions.RECORD_READER_FACTORY, "reader");
        runner.setProperty(Transactions.TERMINALS_TO_GENERATE, "3");
        runner.setProperty(Transactions.MAX_TRANSACTIONS, "2");
        runner.enqueue("ignored");
        runner.run();
        runner.assertTransferCount(Transactions.REL_SUCCESS, 1);
        runner.assertTransferCount(Transactions.REL_ORIGINAL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(Transactions.REL_SUCCESS).get(0);
        out.assertAttributeEquals("record.count", "2");
        assertTrue(out.getContent().contains("Ann") && out.getContent().contains("transaction_id"), out.getContent());
    }
}
