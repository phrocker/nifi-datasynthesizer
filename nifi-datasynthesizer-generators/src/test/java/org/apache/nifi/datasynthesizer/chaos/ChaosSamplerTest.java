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
package org.apache.nifi.datasynthesizer.chaos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mapr.synth.samplers.SchemaSampler;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChaosSamplerTest {

    private static String schema(final String options) {
        return "[{\"name\":\"email\",\"class\":\"chaos\"," + options + ",\"value\":{\"class\":\"email\"}}]";
    }

    @Test
    void rateZeroNeverCorrupts() throws Exception {
        final SchemaSampler sampler = new SchemaSampler(schema("\"rate\":0"));
        for (int i = 0; i < 200; i++) {
            assertTrue(sampler.sample().get("email").asText().contains("@"));
        }
    }

    @Test
    void rateOneAlwaysCorruptsAndRecordsWhenLedgerOpen() throws Exception {
        final SchemaSampler sampler = new SchemaSampler(schema("\"rate\":1,\"faults\":[\"null\"]"));
        ChaosLedger.begin();
        assertTrue(sampler.sample().get("email").isNull());
        final List<FaultRecord> faults = ChaosLedger.drain();
        assertEquals(1, faults.size());
        assertEquals(Fault.NULL, faults.get(0).fault());
        assertEquals("email", faults.get(0).field());
        assertTrue(faults.get(0).original().contains("@"));
        assertFalse(ChaosLedger.isOpen());
    }

    @Test
    void seedMakesRunsReproducible() throws Exception {
        final List<String> a = run(schema("\"rate\":0.5,\"seed\":7"));
        final List<String> b = run(schema("\"rate\":0.5,\"seed\":7"));
        assertEquals(a, b);
        assertNotEquals(a, run(schema("\"rate\":0.5,\"seed\":8")));
    }

    private static List<String> run(final String schema) throws Exception {
        // Underlying email values vary, so compare which samples were corrupted and how.
        final SchemaSampler sampler = new SchemaSampler(schema);
        final List<String> faults = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            ChaosLedger.begin();
            sampler.sample();
            faults.add(ChaosLedger.drain().stream().map(f -> f.fault().name()).findFirst().orElse("-"));
        }
        return faults;
    }

    @Test
    void recordLevelFaultsRejectedInSchema() {
        assertThrows(Exception.class, () -> new SchemaSampler(schema("\"faults\":[\"duplicate\"]")));
    }

    @Test
    void everyFieldFaultProducesSomething() {
        final Random random = new Random(1);
        for (final Fault fault : Fault.FIELD_FAULTS) {
            final JsonNode text = FaultInjector.apply(fault, TextNode.valueOf("hello world"), random, 100);
            final JsonNode number = FaultInjector.apply(fault, IntNode.valueOf(42), random, 100);
            assertTrue(text != null && number != null, fault.name());
            switch (fault) {
                case NULL -> assertTrue(text.isNull() && number.isNull());
                case TYPE -> {
                    assertFalse(text.isTextual(), fault + " " + text);
                    assertFalse(number.isNumber(), fault + " " + number);
                }
                case OVERSIZE -> assertTrue(text.asText().length() >= 100);
                case TRUNCATE -> assertEquals("hello", text.asText());
                case INJECTION -> assertTrue(FaultInjector.INJECTION_PAYLOADS.contains(text.asText()));
                case OUTLIER -> assertTrue(number.isNumber() && number.asLong() != 42);
                default -> { }
            }
        }
        final String shifted = FaultInjector.apply(Fault.OUTLIER, TextNode.valueOf("2026-01-02"), new Random(0), 1).asText();
        assertTrue(shifted.equals("2126-01-02") || shifted.equals("1926-01-02"), shifted);
    }
}
