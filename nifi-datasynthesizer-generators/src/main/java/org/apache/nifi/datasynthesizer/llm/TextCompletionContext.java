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
package org.apache.nifi.datasynthesizer.llm;

import java.util.Objects;
import java.util.Optional;

/**
 * Binds a {@link TextCompletion} provider to the current thread for the duration of a sampling run.
 * <p>
 * Samplers are instantiated by Jackson from schema JSON and therefore cannot receive constructor arguments. A
 * processor or controller service wraps its sampling in {@link #use(TextCompletion)} and any {@code llm} sampler
 * reached from that thread resolves the provider via {@link #current()}.
 */
public final class TextCompletionContext {

    private static final ThreadLocal<TextCompletion> CURRENT = new ThreadLocal<>();

    private TextCompletionContext() {
    }

    /** Binds the provider to the calling thread until the returned scope is closed. Scopes nest. */
    public static Scope use(final TextCompletion completion) {
        Objects.requireNonNull(completion, "completion");
        final TextCompletion previous = CURRENT.get();
        CURRENT.set(completion);
        return new Scope(previous);
    }

    public static Optional<TextCompletion> current() {
        return Optional.ofNullable(CURRENT.get());
    }

    /** Resolves the bound provider or fails with a message that explains how to bind one. */
    public static TextCompletion require() {
        return current().orElseThrow(() -> new TextCompletionException(
                "Schema uses an 'llm' sampler but no Text Generation Service is configured on the processor or controller service"));
    }

    public static final class Scope implements AutoCloseable {
        private final TextCompletion previous;

        private Scope(final TextCompletion previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }
}
