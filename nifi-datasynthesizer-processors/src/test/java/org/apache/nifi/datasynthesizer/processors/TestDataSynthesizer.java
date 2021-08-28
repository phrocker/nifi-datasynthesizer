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
package org.apache.nifi.datasynthesizer.processors;

import org.apache.nifi.datasynthesizer.processors.DataSynthesizer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class TestDataSynthesizer {

    public static final String DEFAULT_COLUMN_FAMILY = "family1";

    private TestRunner getTestRunner(String schema) throws InitializationException {
        MockRecordWriter writerService = new MockRecordWriter("", false);
        final TestRunner runner = TestRunners.newTestRunner(DataSynthesizer.class);
        runner.enforceReadStreamsClosed(false);
        runner.setProperty(DataSynthesizer.SCHEMA,schema);
        runner.setProperty(DataSynthesizer.RECORD_COUNT,"1");
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(DataSynthesizer.RECORD_WRITER,"writer");
        return runner;
    }



    @Test
    public void testSetState() throws Exception {
        String schema = "";
        TestRunner runner = getTestRunner(schema);
        runner.assertNotValid();
        schema = "{\"name\":\"br\", \"class\":\"browser\"}";
        runner = getTestRunner(schema);
        runner.assertValid();
    }

    @Test
    public void testSchema1() throws Exception {
        final String schema = "[{'name':'br', 'class':'browser'}]";
        TestRunner runner = getTestRunner(schema);
        runner = getTestRunner(schema);
        runner.assertValid();
        runner.run();

        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_SUCCESS, 1);
    }

    @Test
    public void testSchema2() throws Exception {
        final String schema = "{'name':'br', 'class':'browser'}";
        TestRunner runner = getTestRunner(schema);
        runner = getTestRunner(schema);
        runner.assertValid();
        runner.run();

        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_SUCCESS, 1);
    }

    @Test
    public void testBrowserSchema() throws Exception {
        final String schema = "{'name':'br', 'class':'browser'}";
        TestRunner runner = getTestRunner(schema);
        runner = getTestRunner(schema);
        runner.assertValid();
        runner.run();

        runner.assertAllFlowFilesTransferred(DataSynthesizer.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DataSynthesizer.REL_SUCCESS).get(0);
        Set<String> expected = new HashSet<>();
        expected.add("Mobile");
        expected.add("Chrome");
        expected.add("Firefox");
        expected.add("Safari");
        expected.add("IE");
        Assert.assertTrue("Does not contain " + out.getContent(), expected.contains(out.getContent().trim()));
    }

}
