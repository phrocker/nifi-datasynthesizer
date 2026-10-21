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

/**
 * Kinds of corruption the {@code chaos} sampler and the record-level chaos profile can inject. Field faults replace
 * one value; record faults act on whole records.
 */
public enum Fault {
    // Field-level
    /** Replaces the value with null. */
    NULL(false),
    /** Replaces text with an empty string, numbers with zero. */
    EMPTY(false),
    /** Swaps the value's type: numbers become non-numeric text, text becomes a number, booleans become text. */
    TYPE(false),
    /** Cuts text roughly in half. */
    TRUNCATE(false),
    /** Pads text to several kilobytes. */
    OVERSIZE(false),
    /** Inserts control characters, zero-width joiners, bidi marks, emoji and homoglyphs into text. */
    UNICODE(false),
    /** Surrounds the value with leading and trailing whitespace, or makes it whitespace only. */
    WHITESPACE(false),
    /** Replaces text with a benign canary payload (SQL, script, path traversal, prompt injection). */
    INJECTION(false),
    /** Pushes numbers far outside their usual range (negated, multiplied) and dates a century away. */
    OUTLIER(false),
    // Record-level
    /** Emits the same record again. */
    DUPLICATE(true),
    /** Suppresses the record. */
    DROP(true),
    /** Adds an unexpected field or removes an existing one. */
    DRIFT(true);

    private final boolean recordLevel;

    Fault(final boolean recordLevel) {
        this.recordLevel = recordLevel;
    }

    public boolean isRecordLevel() {
        return recordLevel;
    }

    /** Field faults that only alter a value without changing its JSON type. */
    public static final java.util.List<Fault> FIELD_FAULTS = java.util.List.of(
            NULL, EMPTY, TYPE, TRUNCATE, OVERSIZE, UNICODE, WHITESPACE, INJECTION, OUTLIER);

    public static Fault parse(final String name) {
        return valueOf(name.trim().toUpperCase().replace('-', '_'));
    }
}
