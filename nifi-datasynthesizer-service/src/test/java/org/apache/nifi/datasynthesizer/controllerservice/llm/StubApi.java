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
package org.apache.nifi.datasynthesizer.controllerservice.llm;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/** A tiny HTTP stub that records request bodies and headers and replies with canned JSON. */
final class StubApi implements AutoCloseable {

    record Captured(String path, Map<String, List<String>> headers, String body) {
    }

    private final HttpServer server;
    final List<Captured> requests = new CopyOnWriteArrayList<>();
    private volatile int status = 200;
    private volatile String response = "{}";

    StubApi() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            requests.add(new Captured(exchange.getRequestURI().getPath(), new java.util.HashMap<>(exchange.getRequestHeaders()), body));
            final byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
    }

    String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    void respond(final int status, final String json) {
        this.status = status;
        this.response = json;
    }

    Captured last() {
        return requests.get(requests.size() - 1);
    }

    List<String> paths() {
        final List<String> paths = new ArrayList<>();
        requests.forEach(r -> paths.add(r.path()));
        return paths;
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
