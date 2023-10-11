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

import io.netty.channel.EventLoopGroup;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class GremlinBootstrapper {

    private static boolean started = false;
    private static boolean disposed = false;

    private static final Logger logger = LoggerFactory.getLogger(GremlinBootstrapper.class);

    // todo: services should not be stored here
    private static ExecutorService gremlinExecutorService;
    private static ServerGremlinExecutor serverGremlinExecutor;
    private static GraphManager graphManager;
    private static final Map<String, MessageSerializer<?>> serializers = new HashMap<>();

    //// AbstractChannelizer copy/paste
    private static final List<Settings.SerializerSettings> DEFAULT_SERIALIZERS = Arrays.asList(
            new Settings.SerializerSettings(GraphSONMessageSerializerV2.class.getName(), Collections.emptyMap()),
            new Settings.SerializerSettings(GraphBinaryMessageSerializerV1.class.getName(), Collections.emptyMap()),
            new Settings.SerializerSettings(GraphBinaryMessageSerializerV1.class.getName(), new HashMap<String, Object>() {{
                put(GraphBinaryMessageSerializerV1.TOKEN_SERIALIZE_RESULT_TO_STRING, true);
            }})
    );

    public static void init(final ServiceContext serviceContext) {
        if (started) return;

        final EventLoopGroup workerGroup = (EventLoopGroup)serviceContext.getService("workerGroup");

        // use the ExecutorService returned from ServerGremlinExecutor as it might be initialized there
        serverGremlinExecutor = (ServerGremlinExecutor)serviceContext.getOrAddService(
                ServerGremlinExecutor.class.getName(),
                () -> new ServerGremlinExecutor(serviceContext.getSettings(), gremlinExecutorService, workerGroup));

        // todo: use serviceContext.getOrAddService
        gremlinExecutorService = serverGremlinExecutor.getGremlinExecutorService();
        graphManager = serverGremlinExecutor.getGraphManager();

        // initialize the OpLoader with configurations being passed to each OpProcessor implementation loaded
        OpLoader.init(serviceContext.getSettings());

        configureSerializers(serviceContext);

        serviceContext.addService("serializers", serializers);

        started = true;
    }

    private static void configureSerializers(final ServiceContext serviceContext) {
        graphManager = serverGremlinExecutor.getGraphManager();

        // grab some sensible defaults if no serializers are present in the config
        final List<Settings.SerializerSettings> serializerSettings =
                (null == serviceContext.getSettings().serializers || serviceContext.getSettings().serializers.isEmpty())
                        ? DEFAULT_SERIALIZERS
                        : serviceContext.getSettings().serializers;

        serializerSettings.stream().map(config -> {
            try {
                final Class clazz = Class.forName(config.className);
                if (!MessageSerializer.class.isAssignableFrom(clazz)) {
                    logger.warn("The {} serialization class does not implement {} - it will not be available.", config.className, MessageSerializer.class.getCanonicalName());
                    return Optional.<MessageSerializer>empty();
                }

                if (clazz.getAnnotation(Deprecated.class) != null)
                    logger.warn("The {} serialization class is deprecated.", config.className);

                final MessageSerializer<?> serializer = (MessageSerializer) clazz.newInstance();
                final Map<String, Graph> graphsDefinedAtStartup = new HashMap<>();
                for (String graphName : serviceContext.getSettings().graphs.keySet()) {
                    graphsDefinedAtStartup.put(graphName, graphManager.getGraph(graphName));
                }

                if (config.config != null)
                    serializer.configure(config.config, graphsDefinedAtStartup);

                return Optional.ofNullable(serializer);
            } catch (ClassNotFoundException cnfe) {
                logger.warn("Could not find configured serializer class - {} - it will not be available", config.className);
                return Optional.<MessageSerializer>empty();
            } catch (Exception ex) {
                logger.warn("Could not instantiate configured serializer class - {} - it will not be available. {}", config.className, ex.getMessage());
                return Optional.<MessageSerializer>empty();
            }
        }).filter(Optional::isPresent).map(Optional::get).flatMap(serializer ->
                Stream.of(serializer.mimeTypesSupported()).map(mimeType -> Pair.with(mimeType, serializer))
        ).forEach(pair -> {
            final String mimeType = pair.getValue0();
            final MessageSerializer<?> serializer = pair.getValue1();
            if (serializers.containsKey(mimeType))
                logger.info("{} already has {} configured - it will not be replaced by {}, change order of serialization configuration if this is not desired.",
                        mimeType, serializers.get(mimeType).getClass().getName(), serializer.getClass().getName());
            else {
                logger.info("Configured {} with {}", mimeType, pair.getValue1().getClass().getName());
                serializers.put(mimeType, serializer);
            }
        });

        if (serializers.size() == 0) {
            logger.error("No serializers were successfully configured - server will not start.");
            throw new RuntimeException("Serialization configuration error.");
        }
    }
}
