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

/**
 * Minimal, framework-free contract for a large language model text completion. The generators module has no
 * NiFi dependency, so NiFi controller services adapt themselves to this interface and hand it to samplers through
 * {@link TextCompletionContext}.
 */
@FunctionalInterface
public interface TextCompletion {

    /**
     * @param system      optional system prompt, may be null
     * @param prompt      user prompt, never null
     * @param maxTokens   upper bound on generated tokens
     * @param temperature optional sampling temperature, may be null when the provider should use its default
     * @return the generated text
     * @throws TextCompletionException when the provider cannot produce text
     */
    String complete(String system, String prompt, int maxTokens, Double temperature);
}
