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

import java.io.UncheckedIOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarkovTextSamplerTest {

    @Test
    void producesRequestedWordCountFromBundledCorpus() throws Exception {
        final SchemaSampler sampler = new SchemaSampler(
                "[{\"name\":\"text\",\"class\":\"markov\",\"corpus\":\"us_const\",\"length\":25}]");
        final JsonNode first = sampler.sample();
        final String text = first.get("text").asText();
        assertEquals(25, text.split(" ").length);
        assertNotEquals(text, sampler.sample().get("text").asText(), "samples should vary");
    }

    @Test
    void defaultCorpusIsAliceOz() throws Exception {
        final SchemaSampler sampler = new SchemaSampler("[{\"name\":\"text\",\"class\":\"markov\",\"length\":10}]");
        assertTrue(sampler.sample().get("text").asText().split(" ").length >= 10);
    }

    @Test
    void unknownCorpusFails() {
        assertThrows(UncheckedIOException.class, () -> MarkovTextSampler.load("no-such-corpus", 3));
    }
}
