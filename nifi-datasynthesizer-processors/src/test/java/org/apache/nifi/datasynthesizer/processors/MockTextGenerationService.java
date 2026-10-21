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
package org.apache.nifi.datasynthesizer.processors;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.datasynthesizer.services.TextGenerationRequest;
import org.apache.nifi.datasynthesizer.services.TextGenerationService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Test double: records requests and answers with a canned function. */
public class MockTextGenerationService extends AbstractControllerService implements TextGenerationService {

    public final List<TextGenerationRequest> requests = new ArrayList<>();
    public final AtomicInteger calls = new AtomicInteger();
    private final Function<TextGenerationRequest, String> answer;

    public MockTextGenerationService() {
        this(request -> "generated text #" + 0);
    }

    public MockTextGenerationService(final Function<TextGenerationRequest, String> answer) {
        this.answer = answer;
    }

    @Override
    public synchronized String generate(final TextGenerationRequest request) throws IOException {
        requests.add(request);
        calls.incrementAndGet();
        return answer.apply(request);
    }
}
