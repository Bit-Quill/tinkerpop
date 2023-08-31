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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class StatusHttpService implements GremlinService {

    private static final Logger logger = LoggerFactory.getLogger(StatusHttpService.class);
    private ServiceContext serviceContext;

    private List<HttpMethod> protocols = Arrays.asList(HttpMethod.GET);
    private static String endpoint = "/status";

    @Override
    public void init(final ServiceContext serviceContext) {
        this.serviceContext = serviceContext;
    }

    @Override
    public void start() {

    }

    @Override
    public boolean canHandle(final Object msg) {
        if (msg instanceof FullHttpRequest) {
            final FullHttpRequest httpRequest = (FullHttpRequest) msg;

            return protocols.contains(httpRequest.method()) && httpRequest.uri().startsWith(endpoint);
        }
        return false;
    }

    @Override
    public boolean handle(final ChannelHandlerContext ctx, final Object msg) {
        final FullHttpRequest req = (FullHttpRequest) msg;
        final boolean keepAlive = HttpUtil.isKeepAlive(req);

        final ByteBuf buf = ctx.alloc().buffer();
        buf.writeBytes("alive TinkerPop 3.8?".getBytes());

        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, buf);
        response.headers().set(CONTENT_TYPE, "text/html; charset=utf-8");

        HttpHandlerUtil.sendAndCleanupConnection(ctx, keepAlive, response);

        return true;
    }

    @Override
    public void shutDown() {

    }

    @Override
    public void tryReleaseResources() {

    }
}
