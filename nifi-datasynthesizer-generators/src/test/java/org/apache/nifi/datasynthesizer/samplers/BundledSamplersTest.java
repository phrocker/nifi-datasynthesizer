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
package org.apache.nifi.datasynthesizer.samplers;

import com.fasterxml.jackson.databind.JsonNode;
import com.mapr.synth.samplers.SchemaSampler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Smoke tests for the vendored log-synth samplers after the Datafaker and resource moves. */
class BundledSamplersTest {

    @Test
    void datafakerBackedSamplers() throws Exception {
        final SchemaSampler sampler = new SchemaSampler("["
                + "{\"name\":\"ip\",\"class\":\"ipv4\"},"
                + "{\"name\":\"quote\",\"class\":\"text\",\"type\":\"chucknorris\"},"
                + "{\"name\":\"imei\",\"class\":\"imei\"}]");
        final JsonNode node = sampler.sample();
        assertTrue(node.get("ip").asText().matches("\\d+\\.\\d+\\.\\d+\\.\\d+"));
        assertFalse(node.get("quote").asText().isBlank());
        assertEquals(15, node.get("imei").asText().length());
    }

    @Test
    void resourceBackedSamplersStillLoad() throws Exception {
        final SchemaSampler sampler = new SchemaSampler("["
                + "{\"name\":\"zip\",\"class\":\"zip\"},"
                + "{\"name\":\"name\",\"class\":\"name\",\"type\":\"first_last\"},"
                + "{\"name\":\"ssn\",\"class\":\"ssn\"},"
                + "{\"name\":\"street\",\"class\":\"street-name\"}]");
        final JsonNode node = sampler.sample();
        final String json = node.toString();
        assertTrue(json.contains("latitude"), json);
        assertFalse(node.get("name").asText().isBlank(), json);
        assertTrue(java.util.regex.Pattern.compile("\\d{3}-\\d{2}-\\d{4}").matcher(json).find(), json);
        assertFalse(node.get("street").asText().isBlank(), json);
    }

    @Test
    void streamBackedSamplersWork() throws Exception {
        // 'word' and 'lookup' used to open classpath resources as java.io.File, which fails inside a jar.
        final SchemaSampler sampler = new SchemaSampler("["
                + "{\"name\":\"w\",\"class\":\"word\"},"
                + "{\"name\":\"tld\",\"class\":\"lookup\",\"resource\":\"tld.csv\"}]");
        for (int i = 0; i < 50; i++) {
            final JsonNode node = sampler.sample();
            assertFalse(node.get("w").asText().isBlank());
            assertFalse(node.get("w").asText().startsWith("w-"), "fell back to synthetic words: " + node);
            assertFalse(node.get("tld").isNull() || node.get("tld").toString().length() < 3, node.toString());
        }
    }

    @Test
    void commuterSamplerProducesTrajectory() throws Exception {
        final SchemaSampler sampler = new SchemaSampler("[{\"name\":\"trip\",\"class\":\"commuter\","
                + "\"home\":{\"class\":\"zip\",\"zip\":\"10001\"},\"work\":5.0}]");
        final JsonNode node = sampler.sample();
        assertTrue(node.get("trip").has("home"));
    }
}
