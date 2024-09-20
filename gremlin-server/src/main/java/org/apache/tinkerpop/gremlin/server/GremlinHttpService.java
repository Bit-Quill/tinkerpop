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
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.apache.tinkerpop.gremlin.server.util.LifeCycleHook;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class GremlinHttpService implements GremlinService {

    private static final Logger logger = LoggerFactory.getLogger(GremlinHttpService.class);
    private ExecutorService gremlinExecutorService;
    private ServerGremlinExecutor serverGremlinExecutor;
    private ServiceContext serviceContext;
    private GraphManager graphManager;
    private GremlinExecutor gremlinExecutor;
    private Map<String, MessageSerializer<?>> serializers = new HashMap<>();
    private HttpGremlinEndpointHandler httpGremlinEndpointHandler;

    private List<HttpMethod> protocols = Arrays.asList(HttpMethod.GET, HttpMethod.POST, HttpMethod.HEAD);
    private static String endpoint = "/text";

    // better to pass all parameters only with serviceContext
    @Override
    public void init(final ServiceContext serviceContext) {
        this.serviceContext = serviceContext;

        GremlinBootstrapper.init(serviceContext);

        serverGremlinExecutor = (ServerGremlinExecutor)serviceContext.getService(ServerGremlinExecutor.class.getName());

        // todo: use serviceContext.getOrAddService
        gremlinExecutorService = serverGremlinExecutor.getGremlinExecutorService();
        graphManager = serverGremlinExecutor.getGraphManager();
        gremlinExecutor = serverGremlinExecutor.getGremlinExecutor();

        serializers = (Map<String, MessageSerializer<?>>)serviceContext.getService("serializers");

        httpGremlinEndpointHandler = buildHandler();
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
        if (msg instanceof FullHttpRequest) {
            final FullHttpRequest httpRequest = (FullHttpRequest) msg;

            return protocols.contains(httpRequest.method()) && httpRequest.uri().startsWith(endpoint);
        }
        return false;
    }

    @Override
    public boolean handle(final ChannelHandlerContext ctx, final Object msg) {
        // hack only for PoC
        httpGremlinEndpointHandler.channelRead(ctx, msg);
        return true;
    }

    // todo: move code to GremlinBootstraper.shutDown
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

    private HttpGremlinEndpointHandler buildHandler() {
        return new HttpGremlinEndpointHandler(serializers, gremlinExecutor, graphManager, serviceContext.getSettings());
    }
}
