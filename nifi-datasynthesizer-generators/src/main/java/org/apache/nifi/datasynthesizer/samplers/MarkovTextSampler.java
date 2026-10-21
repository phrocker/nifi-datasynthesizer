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
import com.fasterxml.jackson.databind.node.TextNode;
import com.mapr.synth.samplers.FieldSampler;
import org.apache.nifi.datasynthesizer.text.MarkovChain;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generates free text from a word-level Markov chain trained on a corpus. Runs entirely offline.
 * <p>
 * Schema example:
 * <pre>
 * {"name": "message", "class": "markov", "corpus": "alice_oz", "length": 40}
 * </pre>
 * {@code corpus} is one of the bundled corpora ({@code alice_oz}, {@code us_const}) or a path to a UTF-8 text
 * file. {@code length} is the number of words to emit (default 128) and {@code order} the chain's key size
 * (default 3). Chains are built once per corpus and shared.
 * <p>
 * Thread safe.
 */
public class MarkovTextSampler extends FieldSampler {

    public static final String DEFAULT_CORPUS = "alice_oz";

    private static final Map<String, MarkovChain> CHAINS = new ConcurrentHashMap<>();

    private String corpus = DEFAULT_CORPUS;
    private int length = 128;
    private int order = 3;

    public MarkovTextSampler() {
    }

    @SuppressWarnings("unused")
    public void setCorpus(final String corpus) {
        this.corpus = corpus;
    }

    @SuppressWarnings("unused")
    public void setLength(final int length) {
        this.length = Math.max(1, length);
    }

    @SuppressWarnings("unused")
    public void setOrder(final int order) {
        if (order < 1) {
            throw new IllegalArgumentException("order must be at least 1");
        }
        this.order = order;
    }

    @Override
    public JsonNode sample() {
        final MarkovChain chain = CHAINS.computeIfAbsent(corpus + "#" + order, key -> load(corpus, order));
        return new TextNode(chain.produce(length));
    }

    static MarkovChain load(final String corpus, final int order) {
        try (InputStream in = open(corpus)) {
            return new MarkovChain(in, order);
        } catch (final IOException e) {
            throw new UncheckedIOException("Unable to load Markov corpus '" + corpus + "'", e);
        }
    }

    private static InputStream open(final String corpus) throws IOException {
        final InputStream bundled = MarkovTextSampler.class.getResourceAsStream("/trainingset/" + corpus + ".txt");
        if (bundled != null) {
            return bundled;
        }
        final Path path = Path.of(corpus);
        if (Files.isRegularFile(path)) {
            return Files.newInputStream(path);
        }
        throw new IOException("Unknown corpus '" + corpus + "'. Use a bundled corpus name or a path to a text file.");
    }
}
