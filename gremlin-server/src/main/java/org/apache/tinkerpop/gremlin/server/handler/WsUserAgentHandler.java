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
package org.apache.tinkerpop.gremlin.server.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.AttributeKey;
import org.apache.tinkerpop.gremlin.driver.util.UserAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler which extracts a user agent header from a web socket handshake if present
 * then logs the user agent and stores it as a channel attribute for future reference.
 */
public class WsUserAgentHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(WsUserAgentHandler.class);
    public static final AttributeKey<String> USER_AGENT_ATTR_KEY = AttributeKey.valueOf(UserAgent.USER_AGENT_HEADER_NAME);

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, java.lang.Object evt){
        if(evt instanceof WebSocketServerProtocolHandler.HandshakeComplete){
            HttpHeaders requestHeaders = ((WebSocketServerProtocolHandler.HandshakeComplete) evt).requestHeaders();
            if(requestHeaders.contains(UserAgent.USER_AGENT_HEADER_NAME)){
                ctx.channel().attr(USER_AGENT_ATTR_KEY).set(requestHeaders.get(UserAgent.USER_AGENT_HEADER_NAME));
                String message = String.format("New Connection on channel [%s] with user agent [%s]", ctx.channel().id().asShortText(), ctx.channel().attr(USER_AGENT_ATTR_KEY).get());
                logger.info(message);
            }
            else{
                logger.info("New Connection on channel [%s] with no user agent provided", ctx.channel().id().asShortText());
            }
        }
        ctx.fireUserEventTriggered(evt);
    }
}
