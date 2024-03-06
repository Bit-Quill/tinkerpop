///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.tinkerpop.gremlin.driver;
//
//import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
//import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
//import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
//import org.apache.hc.client5.http.async.methods.SimpleRequestProducer;
//import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
//import org.apache.hc.client5.http.config.TlsConfig;
//import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
//import org.apache.hc.client5.http.impl.async.MinimalHttpAsyncClient;
//import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
//import org.apache.hc.core5.concurrent.FutureCallback;
//import org.apache.hc.core5.http.HttpHost;
//import org.apache.hc.core5.http.config.Http1Config;
//import org.apache.hc.core5.http.message.StatusLine;
//import org.apache.hc.core5.http.nio.AsyncClientEndpoint;
//import org.apache.hc.core5.http2.HttpVersionPolicy;
//import org.apache.hc.core5.http2.config.H2Config;
//import org.apache.hc.core5.io.CloseMode;
//import org.apache.hc.core5.reactor.IOReactorConfig;
//import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
//import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
//
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
//public class Http2Client extends Client {
//    public Http2Client(Cluster cluster, Settings settings) {
//        super(cluster, settings);
//    }
//
//    @Override
//    public CompletableFuture<ResultSet> submitAsync(final RequestMessage msg) throws ExecutionException, InterruptedException, TimeoutException {
//        final MinimalHttpAsyncClient client = HttpAsyncClients.createMinimal(
//                H2Config.DEFAULT,
//                Http1Config.DEFAULT,
//                IOReactorConfig.DEFAULT,
//                PoolingAsyncClientConnectionManagerBuilder.create()
//                        .setDefaultTlsConfig(TlsConfig.custom()
//                                .setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_2)
//                                .build())
//                        .build());
//
//        client.start();
//
//        // get host from RequestMessage
//        final HttpHost target = new HttpHost("https", "nghttp2.org");
//        final Future<AsyncClientEndpoint> leaseFuture = client.lease(target, null);
//        final AsyncClientEndpoint endpoint = leaseFuture.get(30, TimeUnit.SECONDS);
//        try {
//            final String[] requestUris = new String[] {"/httpbin/ip", "/httpbin/user-agent", "/httpbin/headers"};
//
//            final CountDownLatch latch = new CountDownLatch(requestUris.length);
//            for (final String requestUri: requestUris) {
//                final SimpleHttpRequest request = SimpleRequestBuilder.get()
//                        .setHttpHost(target)
//                        .setPath(requestUri)
//                        .build();
//
//                System.out.println("Executing request " + request);
//                endpoint.execute(
//                        SimpleRequestProducer.create(request),
//                        SimpleResponseConsumer.create(),
//                        new FutureCallback<SimpleHttpResponse>() {
//
//                            @Override
//                            public void completed(final SimpleHttpResponse response) {
//                                latch.countDown();
//                                System.out.println(request + "->" + new StatusLine(response));
//                                System.out.println(response.getBody());
//                            }
//
//                            @Override
//                            public void failed(final Exception ex) {
//                                latch.countDown();
//                                System.out.println(request + "->" + ex);
//                            }
//
//                            @Override
//                            public void cancelled() {
//                                latch.countDown();
//                                System.out.println(request + " cancelled");
//                            }
//
//                        });
//            }
//            latch.await();
//        } finally {
//            endpoint.releaseAndReuse();
//        }
//
//        System.out.println("Shutting down");
//        client.close(CloseMode.GRACEFUL);
//        ////
//
//        return null;
//    }
//
//    @Override
//    protected void initializeImplementation() {
//
//    }
//
//    @Override
//    protected Connection chooseConnection(RequestMessage msg) throws TimeoutException, ConnectionException {
//        return null;
//    }
//
//    @Override
//    public CompletableFuture<Void> closeAsync() {
//        return null;
//    }
//
//    @Override
//    public boolean isClosing() {
//        return false;
//    }
//}
