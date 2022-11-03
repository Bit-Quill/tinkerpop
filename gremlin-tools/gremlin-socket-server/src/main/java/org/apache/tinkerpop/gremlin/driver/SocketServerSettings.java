/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Encapsulates all constants that are needed by SimpleSocketServer. UUID constants are used to coordinate
 * custom response behavior between a test client and the server.
 */
public class SocketServerSettings {
    public final int PORT;

    /**
     * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
     * graph.
     */
    public final UUID SINGLE_VERTEX_REQUEST_ID;

    /**
     * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
     * graph. After some delay, server sends a Close WebSocket frame on the same connection.
     */
    public final UUID SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID;
    public final UUID FAILED_AFTER_DELAY_REQUEST_ID;
    public final UUID CLOSE_CONNECTION_REQUEST_ID;
    public final UUID CLOSE_CONNECTION_REQUEST_ID_2;
    public final UUID RESPONSE_CONTAINS_SERVER_ERROR_REQUEST_ID;

    private static UUID uuidFromString(final String s) {
        return s == null ? null : UUID.fromString(s);
    }

    /**
     * SocketServerSettings are constructed from a yaml config file
     */
    public SocketServerSettings(final Path confFilePath) throws IOException {
        this(Files.newInputStream(confFilePath));
    }

    public SocketServerSettings(final InputStream confInputStream) {
        Objects.requireNonNull(confInputStream);
        Yaml yaml = new Yaml();
        Map<String, Object> settings = yaml.load(confInputStream);
        this.PORT = (int) settings.get("PORT");
        this.SINGLE_VERTEX_REQUEST_ID = uuidFromString((String) settings.get("SINGLE_VERTEX_REQUEST_ID"));
        this.SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID = uuidFromString((String) settings.get("SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID"));
        this.FAILED_AFTER_DELAY_REQUEST_ID = uuidFromString((String) settings.get("FAILED_AFTER_DELAY_REQUEST_ID"));
        this.CLOSE_CONNECTION_REQUEST_ID = uuidFromString((String) settings.get("CLOSE_CONNECTION_REQUEST_ID"));
        this.CLOSE_CONNECTION_REQUEST_ID_2 = uuidFromString((String) settings.get("CLOSE_CONNECTION_REQUEST_ID_2"));
        this.RESPONSE_CONTAINS_SERVER_ERROR_REQUEST_ID = uuidFromString((String) settings.get("RESPONSE_CONTAINS_SERVER_ERROR_REQUEST_ID"));
    }
}
