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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.commons.lang3.SystemUtils;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.ThreadFactoryUtil;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Start and stop Gremlin Server.
 */
public class GremlinServerV4 {

    static {
        // hook slf4j up to netty internal logging
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    private static final String SERVER_THREAD_PREFIX = "gremlin-server-";
    public static final String AUDIT_LOGGER_NAME = "audit.org.apache.tinkerpop.gremlin.server";

    private static final Logger logger = LoggerFactory.getLogger(GremlinServerV4.class);
    private ServiceContext serviceContext;
    private Channel ch;

    private CompletableFuture<Void> serverStopped = null;
    private CompletableFuture<Void> serverStarted = null;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private final boolean isEpollEnabled;

    private ChannelizerV4 channelizer;

    // for PoC only. Should be build from config file
    private GremlinService gremlinHttpService = new GremlinHttpService();
    private GremlinService gremlinWsService = new GremlinWsService();
    private StatusHttpService statusHttpService = new StatusHttpService();

    /**
     * Construct a Gremlin Server instance from {@link Settings}.
     */
    public GremlinServerV4(final Settings settings) {
        settings.optionalMetrics().ifPresent(GremlinServerV4::configureMetrics);

        provideDefaultForGremlinPoolSize(settings);
        this.isEpollEnabled = settings.useEpollEventLoop && SystemUtils.IS_OS_LINUX;
        if (settings.useEpollEventLoop && !SystemUtils.IS_OS_LINUX){
            logger.warn("cannot use epoll in non-linux env, falling back to NIO");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> this.stop().join(), SERVER_THREAD_PREFIX + "shutdown"));

        final ThreadFactory threadFactoryBoss = ThreadFactoryUtil.create("boss-%d");
        // if linux os use epoll else fallback to nio based eventloop
        // epoll helps in reducing GC and has better  performance
        // http://netty.io/wiki/native-transports.html
        if (isEpollEnabled){
            bossGroup = new EpollEventLoopGroup(settings.threadPoolBoss, threadFactoryBoss);
        } else {
            bossGroup = new NioEventLoopGroup(settings.threadPoolBoss, threadFactoryBoss);
        }

        final ThreadFactory threadFactoryWorker = ThreadFactoryUtil.create("worker-%d");
        if (isEpollEnabled) {
            workerGroup = new EpollEventLoopGroup(settings.threadPoolWorker, threadFactoryWorker);
        } else {
            workerGroup = new NioEventLoopGroup(settings.threadPoolWorker, threadFactoryWorker);
        }

        channelizer = new GremlinChannelizerV4();

        buildServiceContext(settings);

        gremlinHttpService.init(serviceContext);
        gremlinWsService.init(serviceContext);
        statusHttpService.init(serviceContext);
    }

    // override if you need to add something
    protected void buildServiceContext(final Settings settings) {
        serviceContext = new ServiceContext(settings);

        serviceContext.addService("workerGroup", workerGroup);
        serviceContext.addService("channelizer", channelizer);
    }

    /**
     * Start Gremlin Server with {@link Settings} provided to the constructor.
     */
    public synchronized CompletableFuture<Void> start() throws Exception {
        if (serverStarted != null) {
            // server already started - don't get it rolling again
            return serverStarted;
        }

        final Settings settings = serviceContext.getSettings();
        serverStarted = new CompletableFuture<>();
        final CompletableFuture<Void> serverReadyFuture = serverStarted;
        try {
            final ServerBootstrap b = new ServerBootstrap();

            // when high value is reached then the channel becomes non-writable and stays like that until the
            // low value is so that there is time to recover
            b.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                    new WriteBufferWaterMark(settings.writeBufferLowWaterMark, settings.writeBufferHighWaterMark));
            b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            gremlinHttpService.start();
            gremlinWsService.start();
            statusHttpService.start();

            channelizer.init(serviceContext, Arrays.asList(gremlinHttpService, gremlinWsService, statusHttpService));

            b.group(bossGroup, workerGroup).childHandler(channelizer);
            if (isEpollEnabled) {
                b.channel(EpollServerSocketChannel.class);
            } else {
                b.channel(NioServerSocketChannel.class);
            }

            // bind to host/port and wait for channel to be ready
            b.bind(serviceContext.getSettings().host, serviceContext.getSettings().port).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(final ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) {
                        ch = channelFuture.channel();

                        logger.info("Gremlin Server configured with worker thread pool of {}, gremlin pool of {} and boss thread pool of {}.",
                                settings.threadPoolWorker, settings.gremlinPool, settings.threadPoolBoss);
                        logger.info("Channel started at port {}.", settings.port);

                        serverReadyFuture.complete(null);
                    } else {
                        serverReadyFuture.completeExceptionally(new IOException(
                                String.format("Could not bind to %s and %s - perhaps something else is bound to that address.", settings.host, settings.port)));
                    }
                }
            });
        } catch (Exception ex) {
            logger.error("Gremlin Server Error", ex);
            serverReadyFuture.completeExceptionally(ex);
        }

        return serverStarted;
    }

    /**
     * Stop Gremlin Server and free the port binding. Note that multiple calls to this method will return the
     * same instance of the {@code CompletableFuture}.
     */
    public synchronized CompletableFuture<Void> stop() {
        if (serverStopped != null) {
            // shutdown has started so don't fire it off again
            return serverStopped;
        }

        serverStopped = new CompletableFuture<>();
        final CountDownLatch servicesLeftToShutdown = new CountDownLatch(3);

        // it's possible that a channel might not be initialized in the first place if bind() fails because
        // of port conflict.  in that case, there's no need to wait for the channel to close.
        if (null == ch)
            servicesLeftToShutdown.countDown();
        else
            ch.close().addListener(f -> servicesLeftToShutdown.countDown());

        logger.info("Shutting down thread pools.");

        try {
            workerGroup.shutdownGracefully().addListener((GenericFutureListener) f -> servicesLeftToShutdown.countDown());
        } finally {
            logger.debug("Shutdown Worker thread pool.");
        }
        try {
            bossGroup.shutdownGracefully().addListener((GenericFutureListener) f -> servicesLeftToShutdown.countDown());
        } finally {
            logger.debug("Shutdown Boss thread pool.");
        }

        gremlinHttpService.shutDown();
        gremlinWsService.shutDown();
        statusHttpService.shutDown();

        // channel is shutdown as are the thread pools - time to kill graphs as nothing else should be acting on them
        new Thread(() -> {

            gremlinHttpService.tryReleaseResources();
            gremlinWsService.tryReleaseResources();
            statusHttpService.tryReleaseResources();

            try {
                servicesLeftToShutdown.await(30000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                logger.warn("Timeout waiting for boss/worker thread pools to shutdown - continuing with shutdown process.");
            }

            // kills reporter threads. this is a last bit of cleanup that can be done. typically, the jvm is headed
            // for shutdown which would obviously kill the reporters, but when it isn't they just keep reporting.
            // removing them all will silent them up and release the appropriate resources.
            MetricManager.INSTANCE.removeAllReporters();

            // removing all the metrics should allow Gremlin Server to clean up the metrics instance so that it can be
            // started again in the same JVM without those metrics initialized which generates a warning and won't
            // reset to start values
            MetricManager.INSTANCE.removeAllMetrics();

            logger.info("Gremlin Server - shutdown complete");
            serverStopped.complete(null);
        }, SERVER_THREAD_PREFIX + "stop").start();

        return serverStopped;
    }

    public ChannelHandler getChannelizer() {
        return channelizer;
    }

    public static void main(final String[] args) throws Exception {
        // add to vm options: -Dlogback.configurationFile=file:conf/logback.xml
        printHeader();
        final String file;
        if (args.length > 0)
            file = args[0];
        else
            file = "conf/gremlin-server.yaml";

        final Settings settings;
        try {
            settings = Settings.read(file);
        } catch (Exception ex) {
            logger.error("Configuration file at {} could not be found or parsed properly. [{}]", file, ex.getMessage());
            return;
        }

        logger.info("Configuring Gremlin Server from {}", file);
        final GremlinServerV4 server = new GremlinServerV4(settings);
        server.start().exceptionally(t -> {
            logger.error("Gremlin Server was unable to start and will now begin shutdown: {}", t.getMessage());
            server.stop().join();
            return null;
        }).join();
    }

    public static String getHeader() {
        final StringBuilder builder = new StringBuilder();
        builder.append(Gremlin.version() + "\r\n");
        builder.append("         \\,,,/\r\n");
        builder.append("         (o o)\r\n");
        builder.append("-----oOOo-(3)-oOOo-----\r\n");
        return builder.toString();
    }

    private static void configureMetrics(final Settings.ServerMetrics settings) {
        final MetricManager metrics = MetricManager.INSTANCE;
        settings.optionalConsoleReporter().ifPresent(config -> {
            if (config.enabled) metrics.addConsoleReporter(config.interval);
        });

        settings.optionalCsvReporter().ifPresent(config -> {
            if (config.enabled) metrics.addCsvReporter(config.interval, config.fileName);
        });

        settings.optionalJmxReporter().ifPresent(config -> {
            if (config.enabled) metrics.addJmxReporter(config.domain, config.agentId);
        });

        settings.optionalSlf4jReporter().ifPresent(config -> {
            if (config.enabled) metrics.addSlf4jReporter(config.interval, config.loggerName);
        });

        settings.optionalGangliaReporter().ifPresent(config -> {
            if (config.enabled) {
                try {
                    metrics.addGangliaReporter(config.host, config.port,
                            config.addressingMode, config.ttl, config.protocol31, config.hostUUID, config.spoof, config.interval);
                } catch (IOException ioe) {
                    logger.warn("Error configuring the Ganglia Reporter.", ioe);
                }
            }
        });

        settings.optionalGraphiteReporter().ifPresent(config -> {
            if (config.enabled) metrics.addGraphiteReporter(config.host, config.port, config.prefix, config.interval);
        });
    }

    private static void printHeader() {
        logger.info(getHeader());
    }

    private static void provideDefaultForGremlinPoolSize(final Settings settings) {
        if (settings.gremlinPool == 0)
            settings.gremlinPool = Runtime.getRuntime().availableProcessors();
    }

    @Override
    public String toString() {
        return "GremlinServer " + serviceContext.getSettings().host + ":" + serviceContext.getSettings().port;
    }
}
