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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.server.handler.OpExecutorHandler;
import org.apache.tinkerpop.gremlin.server.handler.OpSelectorHandler;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.apache.tinkerpop.gremlin.server.util.LifeCycleHook;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class GremlinWsService implements GremlinService {

    private static final Logger logger = LoggerFactory.getLogger(GremlinWsService.class);
    private ExecutorService gremlinExecutorService;
    private ServerGremlinExecutor serverGremlinExecutor;
    private ServiceContext serviceContext;
    private GraphManager graphManager;
    private GremlinExecutor gremlinExecutor;
    private ScheduledExecutorService scheduledExecutorService;
    private final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
    private OpSelectorHandler opSelectorHandler;
    private OpExecutorHandler opExecutorHandler;

    private List<HttpMethod> protocols = Arrays.asList();
    private static String endpoint = "/gremlin";

    @Override
    public void init(final ServiceContext serviceContext) {
        this.serviceContext = serviceContext;

        GremlinBootstrapper.init(serviceContext);

        final EventLoopGroup workerGroup = (EventLoopGroup)serviceContext.getService("workerGroup");

        // use the ExecutorService returned from ServerGremlinExecutor as it might be initialized there
        serverGremlinExecutor = (ServerGremlinExecutor)serviceContext.getOrAddService(
                ServerGremlinExecutor.class.getName(),
                () -> new ServerGremlinExecutor(serviceContext.getSettings(), gremlinExecutorService, workerGroup));

        // todo: use serviceContext.getOrAddService
        gremlinExecutorService = serverGremlinExecutor.getGremlinExecutorService();
        graphManager = serverGremlinExecutor.getGraphManager();
        gremlinExecutor = serverGremlinExecutor.getGremlinExecutor();
        scheduledExecutorService = serverGremlinExecutor.getScheduledExecutorService();

        // initialize the OpLoader with configurations being passed to each OpProcessor implementation loaded
        OpLoader.init(serviceContext.getSettings());

        configureSerializers();

        opSelectorHandler = new OpSelectorHandler(serviceContext.getSettings(), graphManager, gremlinExecutor,
                scheduledExecutorService, (ChannelizerV4)serviceContext.getService("channelizer"));
        opExecutorHandler = new OpExecutorHandler(serviceContext.getSettings(), graphManager, gremlinExecutor, scheduledExecutorService);
    }

    @Override
    public void start() {
        // fire off any lifecycle scripts that were provided by the user. hooks get initialized during
        // ServerGremlinExecutor initialization
        serverGremlinExecutor.getHooks().forEach(hook -> {
            logger.info("Executing start up {}", LifeCycleHook.class.getSimpleName());
            try {
                hook.onStartUp(new LifeCycleHook.Context(logger));
            } catch (UnsupportedOperationException uoe) {
                // if the user doesn't implement onStartUp the scriptengine will throw
                // this exception.  it can safely be ignored.
            }
        });
    }

    @Override
    public boolean canHandle(final ChannelHandlerContext ctx, final Object msg) {
        // RequestMessage
        return msg instanceof RequestMessage;
    }

    @Override
    public boolean handle(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        // hack only for PoC
        final Pair<RequestMessage, ThrowingConsumer<Context>> selectOp = opSelectorHandler.selectOp(ctx, (RequestMessage)msg);
        opExecutorHandler.handle(ctx, selectOp);
        return true;
    }

    @Override
    public void shutDown() {
        // release resources in the OpProcessors (e.g. kill sessions)
        OpLoader.getProcessors().entrySet().forEach(kv -> {
            logger.info("Shutting down OpProcessor[{}]", kv.getKey());
            try {
                kv.getValue().close();
            } catch (Exception ex) {
                logger.warn("Shutdown will continue but, there was an error encountered while closing " + kv.getKey(), ex);
            }
        });

        try {
            if (gremlinExecutorService != null) gremlinExecutorService.shutdown();
        } finally {
            logger.debug("Shutdown Gremlin thread pool.");
        }
    }

    @Override
    public void tryReleaseResources() {
        if (serverGremlinExecutor != null) {
            serverGremlinExecutor.getHooks().forEach(hook -> {
                logger.info("Executing shutdown {}", LifeCycleHook.class.getSimpleName());
                try {
                    hook.onShutDown(new LifeCycleHook.Context(logger));
                } catch (UnsupportedOperationException | UndeclaredThrowableException uoe) {
                    // if the user doesn't implement onShutDown the scriptengine will throw
                    // this exception.  it can safely be ignored.
                }
            });
        }

        try {
            if (gremlinExecutorService != null) {
                if (!gremlinExecutorService.awaitTermination(30000, TimeUnit.MILLISECONDS)) {
                    logger.warn("Gremlin thread pool did not fully terminate - continuing with shutdown process");
                }
            }
        } catch (InterruptedException ie) {
            logger.warn("Timeout waiting for Gremlin thread pool to shutdown - continuing with shutdown process.");
        }

        // close TraversalSource and Graph instances - there aren't guarantees that closing Graph will close all
        // spawned TraversalSource instances so both should be closed directly and independently.
        if (serverGremlinExecutor != null) {
            final Set<String> traversalSourceNames = serverGremlinExecutor.getGraphManager().getTraversalSourceNames();
            traversalSourceNames.forEach(traversalSourceName -> {
                logger.debug("Closing GraphTraversalSource instance [{}]", traversalSourceName);
                try {
                    serverGremlinExecutor.getGraphManager().getTraversalSource(traversalSourceName).close();
                } catch (Exception ex) {
                    logger.warn(String.format("Exception while closing GraphTraversalSource instance [%s]", traversalSourceName), ex);
                } finally {
                    logger.info("Closed GraphTraversalSource instance [{}]", traversalSourceName);
                }

                try {
                    serverGremlinExecutor.getGraphManager().removeTraversalSource(traversalSourceName);
                } catch (Exception ex) {
                    logger.warn(String.format("Exception while removing GraphTraversalSource instance [%s] from GraphManager", traversalSourceName), ex);
                }
            });

            final Set<String> graphNames = serverGremlinExecutor.getGraphManager().getGraphNames();
            graphNames.forEach(gName -> {
                logger.debug("Closing Graph instance [{}]", gName);
                try {
                    final Graph graph = serverGremlinExecutor.getGraphManager().getGraph(gName);
                    graph.close();
                } catch (Exception ex) {
                    logger.warn(String.format("Exception while closing Graph instance [%s]", gName), ex);
                } finally {
                    logger.info("Closed Graph instance [{}]", gName);
                }

                try {
                    serverGremlinExecutor.getGraphManager().removeGraph(gName);
                } catch (Exception ex) {
                    logger.warn(String.format("Exception while removing Graph instance [%s] from GraphManager", gName), ex);
                }
            });
        }
    }

    //// AbstractChannelizer copy/paste
    protected static final List<Settings.SerializerSettings> DEFAULT_SERIALIZERS = Arrays.asList(
            new Settings.SerializerSettings(GraphSONMessageSerializerV2.class.getName(), Collections.emptyMap()),
            new Settings.SerializerSettings(GraphBinaryMessageSerializerV1.class.getName(), Collections.emptyMap()),
            new Settings.SerializerSettings(GraphBinaryMessageSerializerV1.class.getName(), new HashMap<String,Object>(){{
                put(GraphBinaryMessageSerializerV1.TOKEN_SERIALIZE_RESULT_TO_STRING, true);
            }})
    );

    private void configureSerializers() {
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
