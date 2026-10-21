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
 * One injected fault, recorded so a test can assert that the pipeline under test caught it.
 *
 * @param fault    what was done
 * @param field    the field affected, or null for record-level faults
 * @param original the value before corruption as text, or null
 * @param injected the value after corruption as text, or null
 */
public record FaultRecord(Fault fault, String field, String original, String injected) {

    static String describe(final com.fasterxml.jackson.databind.JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        final String text = node.isTextual() ? node.asText() : node.toString();
        return text.length() > 256 ? text.substring(0, 253) + "..." : text;
    }
}
