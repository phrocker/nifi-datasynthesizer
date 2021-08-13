/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.nifi.datasynthesizer.processors.synthesizers.telephony;

import org.apache.nifi.datasynthesizer.processors.DataSynthesizer;
import org.apache.nifi.datasynthesizer.processors.synthesizers.telephony.TextMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestTextMessage {


    private TestRunner getTestRunner(String area_code) throws InitializationException {
        MockRecordWriter writerService = new MockRecordWriter("", false);
        final TestRunner runner = TestRunners.newTestRunner(TextMessage.class);
        runner.enforceReadStreamsClosed(false);
        if (null != area_code) {
            runner.setProperty(TextMessage.SOURCE_AREA_CODE, area_code);
        }
        runner.setProperty(DataSynthesizer.RECORD_COUNT,"1");
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(DataSynthesizer.RECORD_WRITER,"writer");
        return runner;
    }




    @Test
    public void testValidPhoneNumberWithAreaCode() throws Exception {
        final String schema = "{'name':'br', 'class':'browser'}";
        TestRunner runner = getTestRunner(schema);
        runner = getTestRunner("497");
        runner.assertValid();
        runner.run();

        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0);
        String phoneNumber = out.getContent().trim();
        System.out.println(phoneNumber);
    }


}
