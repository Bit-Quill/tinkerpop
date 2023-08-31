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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.GremlinResponseFrameEncoder;
import org.apache.tinkerpop.gremlin.server.handler.HttpBasicAuthorizationHandler;
import org.apache.tinkerpop.gremlin.server.handler.OpExecutorHandler;
import org.apache.tinkerpop.gremlin.server.handler.OpSelectorHandler;
import org.apache.tinkerpop.gremlin.server.handler.RoutingHandler;
import org.apache.tinkerpop.gremlin.server.handler.WsGremlinBinaryRequestDecoder;
import org.apache.tinkerpop.gremlin.server.handler.WsGremlinCloseRequestDecoder;
import org.apache.tinkerpop.gremlin.server.handler.WsGremlinResponseFrameEncoder;
import org.apache.tinkerpop.gremlin.server.handler.WsGremlinTextRequestDecoder;
import org.apache.tinkerpop.gremlin.server.handler.WsUserAgentHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

/**

 */
public class GremlinChannelizerV4 extends ChannelInitializer<SocketChannel> implements ChannelizerV4 {

    // todo: move out?
    public static final String PIPELINE_REQUEST_HANDLER = "request-handler";
    public static final String PIPELINE_HTTP_RESPONSE_ENCODER = "http-response-encoder";
    public static final String PIPELINE_HTTP_AGGREGATOR = "http-aggregator";
    public static final String PIPELINE_WEBSOCKET_SERVER_COMPRESSION = "web-socket-server-compression-handler";
    protected static final String PIPELINE_HTTP_REQUEST_DECODER = "http-request-decoder";

    private static final Logger logger = LoggerFactory.getLogger(GremlinChannelizerV4.class);

    protected ServiceContext serviceContext;
    protected RoutingHandler routingHandler;

    @Override
    public void init(final ServiceContext serviceContext, final List<GremlinService> services) {
        this.serviceContext = serviceContext;
        routingHandler = new RoutingHandler(services);
    }

    @Override
    public void initChannel(final SocketChannel ch) throws Exception {
        final ChannelPipeline pipeline = ch.pipeline();

        if (supportsIdleMonitor()) {
            final int idleConnectionTimeout = (int) (serviceContext.getSettings().idleConnectionTimeout / 1000);
            final int keepAliveInterval = (int) (serviceContext.getSettings().keepAliveInterval / 1000);
            pipeline.addLast(new IdleStateHandler(idleConnectionTimeout, keepAliveInterval, 0));
        }

        // configureHttpPipeline(pipeline);

        initWs();
        configureWsPipeline(pipeline);
    }

    /// ----------- WS setup
    private GremlinResponseFrameEncoder gremlinResponseFrameEncoder;
    private WsGremlinTextRequestDecoder wsGremlinTextRequestDecoder;
    private WsGremlinBinaryRequestDecoder wsGremlinBinaryRequestDecoder;
    private WsGremlinResponseFrameEncoder wsGremlinResponseFrameEncoder;
    private WsGremlinCloseRequestDecoder wsGremlinCloseRequestDecoder;

    public void initWs() {
        // all encoders should be wrapped as services
        gremlinResponseFrameEncoder = new GremlinResponseFrameEncoder();
        wsGremlinTextRequestDecoder = new WsGremlinTextRequestDecoder(serviceContext);
        wsGremlinBinaryRequestDecoder = new WsGremlinBinaryRequestDecoder(serviceContext);
        wsGremlinCloseRequestDecoder = new WsGremlinCloseRequestDecoder(serviceContext);
        wsGremlinResponseFrameEncoder = new WsGremlinResponseFrameEncoder();
    }

    public void configureWsPipeline(final ChannelPipeline pipeline) {
        final Settings settings = serviceContext.getSettings();

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-encoder-aggregator", LogLevel.DEBUG));

        pipeline.addLast(PIPELINE_HTTP_RESPONSE_ENCODER, new HttpResponseEncoder());

        logger.debug("HttpRequestDecoder settings - maxInitialLineLength={}, maxHeaderSize={}, maxChunkSize={}",
                settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize);
        pipeline.addLast(PIPELINE_HTTP_REQUEST_DECODER, new HttpRequestDecoder(settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize));

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-decoder-aggregator", LogLevel.DEBUG));

        logger.debug("HttpObjectAggregator settings - maxContentLength={}, maxAccumulationBufferComponents={}",
                settings.maxContentLength, settings.maxAccumulationBufferComponents);
        final HttpObjectAggregator aggregator = new HttpObjectAggregator(settings.maxContentLength);
        aggregator.setMaxCumulationBufferComponents(settings.maxAccumulationBufferComponents);
        pipeline.addLast(PIPELINE_HTTP_AGGREGATOR, aggregator);
        // Add compression extension for WebSocket defined in https://tools.ietf.org/html/rfc7692
        pipeline.addLast(PIPELINE_WEBSOCKET_SERVER_COMPRESSION, new WebSocketServerCompressionHandler());

        // setting closeOnProtocolViolation to false prevents causing all the other requests using the same channel
        // to fail when a single request causes a protocol violation.
        // todo: collect all ws services path
        final WebSocketDecoderConfig wsDecoderConfig = WebSocketDecoderConfig.newBuilder().
                closeOnProtocolViolation(false).allowExtensions(true).maxFramePayloadLength(settings.maxContentLength).build();
        pipeline.addLast(PIPELINE_REQUEST_HANDLER, new WebSocketServerProtocolHandler("/gremlin",
                null, false, false, 10000L, wsDecoderConfig));

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

        pipeline.addLast("ws-frame-encoder", wsGremlinResponseFrameEncoder);
        pipeline.addLast("response-frame-encoder", gremlinResponseFrameEncoder);
        pipeline.addLast("request-text-decoder", wsGremlinTextRequestDecoder);
        pipeline.addLast("request-binary-decoder", wsGremlinBinaryRequestDecoder);
        pipeline.addLast("request-close-decoder", wsGremlinCloseRequestDecoder);

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

        pipeline.addLast("http-routing-handler", routingHandler);
    }

    public void configureHttpPipeline(final ChannelPipeline pipeline) {
        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-io", LogLevel.DEBUG));

        pipeline.addLast("http-server", new HttpServerCodec());

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("http-io", LogLevel.DEBUG));

        final HttpObjectAggregator aggregator = new HttpObjectAggregator(serviceContext.getSettings().maxContentLength);
        aggregator.setMaxCumulationBufferComponents(serviceContext.getSettings().maxAccumulationBufferComponents);
        pipeline.addLast(PIPELINE_HTTP_AGGREGATOR, aggregator);

        pipeline.addLast("http-routing-handler", routingHandler);
    }

    @Override
    public void init(ServerGremlinExecutor serverGremlinExecutor) {
        // remove
    }
}
