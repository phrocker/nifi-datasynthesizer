/*
 * Licensed to the Ted Dunning under one or more contributor license
 * agreements.  See the NOTICE file that may be
 * distributed with this work for additional information
 * regarding copyright ownership.  Ted Dunning licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.mapr.synth.samplers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mapr.synth.distributions.TermGenerator;
import com.mapr.synth.distributions.WordGenerator;

/**
 * Sample from English words with somewhat plausible frequency distribution.
 */
public class WordSampler extends FieldSampler {
    private final TermGenerator gen = new TermGenerator(new WordGenerator("word-frequency-seed", "other-words"), 1, 0.8);
    public WordSampler() {
    }

    @Override
    public JsonNode sample() {
        return new TextNode(gen.sample());
    }
}
