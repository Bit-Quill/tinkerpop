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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.List;
import java.util.UUID;


/**
 * Initializer which partially mimics the Gremlin Server. This initializer injects a handler in the
 * server pipeline that can be modified to send the desired response for a test case.
 */
public class TestWSGremlinInitializer extends TestWebSocketServerInitializer {
    private static final Logger logger = LoggerFactory.getLogger(TestWSGremlinInitializer.class);

    private final SocketServerSettings settings;

    /**
     * Gremlin serializer used for serializing/deserializing the request/response. This should be same as client.
     */
    private static final GraphSONMessageSerializerV2d0 SERIALIZER = new GraphSONMessageSerializerV2d0();

    public TestWSGremlinInitializer(SocketServerSettings settings) {
        this.settings = settings;
    }

    @Override
    public void postInit(ChannelPipeline pipeline) {
        pipeline.addLast(new ClientTestConfigurableHandler());
    }

    /**
     * Handler introduced in the server pipeline to configure expected response for test cases.
     */
    private class ClientTestConfigurableHandler extends MessageToMessageDecoder<BinaryWebSocketFrame> {
        @Override
        protected void decode(final ChannelHandlerContext ctx, final BinaryWebSocketFrame frame, final List<Object> objects)
                throws Exception {
            final ByteBuf messageBytes = frame.content();
            final byte len = messageBytes.readByte();
            if (len <= 0) {
                objects.add(RequestMessage.INVALID);
                return;
            }

            final ByteBuf contentTypeBytes = ctx.alloc().buffer(len);
            try {
                messageBytes.readBytes(contentTypeBytes);
            } finally {
                contentTypeBytes.release();
            }
            final RequestMessage msg = SERIALIZER.deserializeRequest(messageBytes.discardReadBytes());

            if (msg.getRequestId().equals(settings.SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID)) {
                logger.info("sending vertex result frame");
                ctx.channel().writeAndFlush(new TextWebSocketFrame(returnSingleVertexResponse(
                        settings.SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID)));
                logger.info("waiting for 2 sec");
                Thread.sleep(2000);
                logger.info("sending close frame");
                ctx.channel().writeAndFlush(new CloseWebSocketFrame());
            } else if (msg.getRequestId().equals(settings.SINGLE_VERTEX_REQUEST_ID)) {
                logger.info("sending vertex result frame");
                ctx.channel().writeAndFlush(new TextWebSocketFrame(returnSingleVertexResponse(settings.SINGLE_VERTEX_REQUEST_ID)));
            } else if (msg.getRequestId().equals(settings.FAILED_AFTER_DELAY_REQUEST_ID)) {
                logger.info("waiting for 2 sec");
                Thread.sleep(1000);
                final ResponseMessage responseMessage = ResponseMessage.build(msg)
                        .code(ResponseStatusCode.SERVER_ERROR)
                        .statusAttributeException(new RuntimeException()).create();
                ctx.channel().writeAndFlush(new TextWebSocketFrame(SERIALIZER.serializeResponseAsString(responseMessage)));
            } else if (msg.getRequestId().equals(settings.CLOSE_CONNECTION_REQUEST_ID)) {
                Thread.sleep(1000);
                ctx.channel().writeAndFlush(new CloseWebSocketFrame());
            } else if (msg.getRequestId().equals(settings.RESPONSE_CONTAINS_SERVER_ERROR_REQUEST_ID)) {
                Thread.sleep(1000);
                ctx.channel().writeAndFlush(new CloseWebSocketFrame());
            }
        }

        private String returnSingleVertexResponse(final UUID requestID) throws SerializationException {
            final TinkerGraph graph = TinkerFactory.createClassic();
            final GraphTraversalSource g = graph.traversal();
            final Vertex t = g.V().limit(1).next();

            return SERIALIZER.serializeResponseAsString(ResponseMessage.build(requestID).result(t).create());
        }
    }
}
