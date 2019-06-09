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
package org.poma.accumulo.nifi.processors;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
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
import org.poma.accumulo.nifi.controllerservices.AccumuloService;
import org.poma.accumulo.nifi.controllerservices.MockAccumuloService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class TestScanAccumulo {

    public static final String DEFAULT_COLUMN_FAMILY = "family1";

    private static MiniAccumuloCluster accumulo;



    private TestRunner getTestRunner(String table, String columnFamily) {
        final TestRunner runner = TestRunners.newTestRunner(ScanAccumulo.class);
        runner.enforceReadStreamsClosed(false);
        runner.setProperty(ScanAccumulo.TABLE_NAME, table);
        return runner;
    }




    @BeforeClass
    public static void setupInstance() throws IOException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException {
        Path tempDirectory = Files.createTempDirectory("acc"); // JUnit and Guava supply mechanisms for creating temp directories
        accumulo = new MiniAccumuloCluster(tempDirectory.toFile(), "password");
        accumulo.start();
    }

    private Set<Key> generateTestData(TestRunner runner, String table, boolean valueincq,String delim, String cv) throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        BatchWriterConfig writerConfig = new BatchWriterConfig();
        writerConfig.setMaxWriteThreads(2);
        writerConfig.setMaxMemory(1024*1024);
        MultiTableBatchWriter writer  = accumulo.getConnector("root","password").createMultiTableBatchWriter(writerConfig);

        long ts = System.currentTimeMillis();


        final MockRecordWriter parser = new MockRecordWriter();
        try {
            runner.addControllerService("parser", parser);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.enableControllerService(parser);
        runner.setProperty(ScanAccumulo.RECORD_WRITER,"parser");


        Set<Key> expectedKeys = new HashSet<>();
        ColumnVisibility colViz = new ColumnVisibility();
        if (null != cv)
            colViz = new ColumnVisibility(cv);
        Random random = new Random();
        for (int x = 0; x < 5; x++) {
            //final int row = random.nextInt(10000000);
            final String row = UUID.randomUUID().toString();
            final String cf = UUID.randomUUID().toString();
            final String cq = UUID.randomUUID().toString();
            Text keyCq = new Text("name");
            if (valueincq){
                if (null != delim && !delim.isEmpty())
                    keyCq.append(delim.getBytes(),0,delim.length());
                keyCq.append(cf.getBytes(),0,cf.length());
            }
            expectedKeys.add(new Key(new Text(row), new Text("family1"), keyCq, colViz,ts));
            keyCq = new Text("code");
            if (valueincq){
                if (null != delim && !delim.isEmpty())
                    keyCq.append(delim.getBytes(),0,delim.length());
                keyCq.append(cq.getBytes(),0,cq.length());
            }
            expectedKeys.add(new Key(new Text(row), new Text("family1"), keyCq, colViz, ts));
            Mutation m = new Mutation(row);
            m.put(new Text("family1"),new Text(keyCq),colViz,ts, new Value());
            writer.getBatchWriter(table).addMutation(m);
        }
        writer.flush();
        return expectedKeys;
    }

    void verifyKey(String tableName, Set<Key> expectedKeys, Authorizations auths) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (null == auths)
            auths = new Authorizations();
        try(BatchScanner scanner = accumulo.getConnector("root","password").createBatchScanner(tableName,auths,1)) {
            List<Range> ranges = new ArrayList<>();
            ranges.add(new Range());
            scanner.setRanges(ranges);
            for (Map.Entry<Key, Value> kv : scanner) {
                Assert.assertTrue(kv.getKey() + " not in expected keys",expectedKeys.remove(kv.getKey()));
            }
        }
        Assert.assertEquals(0, expectedKeys.size());

    }

    private void basicPutSetup(boolean valueincq) throws Exception {
        basicPutSetup(valueincq,null,null,null,false);
    }

    private void basicPutSetup(boolean valueincq, final String delim) throws Exception {
        basicPutSetup(valueincq,delim,null,null,false);
    }

    private void basicPutSetup(boolean valueincq,String delim, String auths, Authorizations defaultVis, boolean deletes) throws Exception {
        String tableName = UUID.randomUUID().toString();
        tableName=tableName.replace("-","a");
        accumulo.getConnector("root","password").tableOperations().create(tableName);
        if (null != defaultVis)
        accumulo.getConnector("root","password").securityOperations().changeUserAuthorizations("root",defaultVis);
        TestRunner runner = getTestRunner(tableName, DEFAULT_COLUMN_FAMILY);
        runner.setProperty(ScanAccumulo.START_KEY, "");
        runner.setProperty(ScanAccumulo.AUTHORIZATIONS, "");
        runner.setProperty(ScanAccumulo.END_KEY, "");

        AccumuloService client = MockAccumuloService.getService(runner,accumulo.getZooKeepers(),accumulo.getInstanceName(),"root","password");
        Set<Key> expectedKeys = generateTestData(runner,tableName,valueincq,delim, auths);
        runner.enqueue("Test".getBytes("UTF-8")); // This is to coax the processor into reading the data in the reader.l
        runner.run();


        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);
        for(MockFlowFile ff : results){
            String attr = ff.getAttribute("record.count");
            Assert.assertEquals(5,Integer.valueOf(attr).intValue());
        }
        Assert.assertTrue("Wrong count, received " + results.size(), results.size() == 1);
    }




    @Test
    public void testPullData() throws Exception {
        basicPutSetup(false);
    }



}
