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
package org.apache.tinkerpop.gremlin.server;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class ServiceContext {
    private final Settings settings;
    private final Map<String, Object> serviceRegistry = new HashMap<>();

    public ServiceContext(Settings settings) {
        this.settings = settings;
    }

    public Settings getSettings() {
        return settings;
    }

    public Object getOrAddService(final String serviceName, final Supplier<Object> supplier) {
        if(!serviceRegistry.containsKey(serviceName)) {
            serviceRegistry.put(serviceName, supplier.get());
        }
        return serviceRegistry.get(serviceName);
    }

    public Object getService(final String serviceName) {
        if(!serviceRegistry.containsKey(serviceName)) {
            throw new RuntimeException("todo");
        }
        return serviceRegistry.get(serviceName);
    }

    public void addService(final String serviceName, Object service) {
        serviceRegistry.put(serviceName, service);
    }
}
