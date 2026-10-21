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
package org.apache.nifi.datasynthesizer.services;

import org.apache.nifi.controller.ControllerService;

import java.io.IOException;

/**
 * Generates free text with a large language model. Implementations wrap one provider (Anthropic, an OpenAI-compatible
 * endpoint, ...). Processors hand the service to {@code llm} schema samplers.
 */
public interface TextGenerationService extends ControllerService {

    /**
     * @param request what to generate
     * @return the generated text, never null
     * @throws IOException when the provider cannot be reached or declines the request
     */
    String generate(TextGenerationRequest request) throws IOException;
}
