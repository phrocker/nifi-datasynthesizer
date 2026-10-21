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
package org.apache.nifi.datasynthesizer.text;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Word-level Markov chain text generator.
 * <p>
 * Adapted from the public domain example at https://rosettacode.org/wiki/Markov_chain_text_generator#Java
 * <p>
 * Immutable after construction, so safe to share between threads.
 */
public class MarkovChain {

    private final Map<String, List<String>> transitions = new HashMap<>();
    private final List<String> keys = new ArrayList<>();
    private final int keySize;

    public MarkovChain(final InputStream corpus) throws IOException {
        this(corpus, 3);
    }

    public MarkovChain(final InputStream corpus, final int keySize) throws IOException {
        if (keySize < 1) {
            throw new IllegalArgumentException("keySize must be at least 1");
        }
        this.keySize = keySize;
        train(corpus);
        if (keys.isEmpty()) {
            throw new IOException("Corpus is too short to build a Markov chain of order " + keySize);
        }
    }

    private void train(final InputStream stream) throws IOException {
        final String text = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        final String[] words = text.trim().split("\\s+");
        for (int i = 0; i + keySize < words.length; i++) {
            final StringBuilder key = new StringBuilder(words[i]);
            for (int j = i + 1; j < i + keySize; j++) {
                key.append(' ').append(words[j]);
            }
            final String k = key.toString();
            final List<String> next = transitions.computeIfAbsent(k, unused -> {
                keys.add(k);
                return new ArrayList<>();
            });
            next.add(words[i + keySize]);
        }
    }

    /** Produces roughly {@code wordCount} words starting from a random position in the corpus. */
    public String produce(final int wordCount) {
        final Random random = ThreadLocalRandom.current();
        String key = keys.get(random.nextInt(keys.size()));
        final StringBuilder out = new StringBuilder(key);
        int emitted = keySize;
        while (emitted < wordCount) {
            final List<String> next = transitions.get(key);
            if (next == null || next.isEmpty()) {
                key = keys.get(random.nextInt(keys.size()));
                out.append(' ').append(key);
                emitted += keySize;
                continue;
            }
            final String word = next.get(random.nextInt(next.size()));
            out.append(' ').append(word);
            emitted++;
            final int firstSpace = key.indexOf(' ');
            key = keySize == 1 ? word : key.substring(firstSpace + 1) + ' ' + word;
        }
        return out.toString();
    }
}
