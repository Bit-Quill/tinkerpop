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

import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase2;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


@Warmup(time = 2001, timeUnit = MILLISECONDS, iterations = 1)
@Measurement(time = 2002, timeUnit = MILLISECONDS, iterations = 2)
//@Warmup(iterations = 1)
//@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
public class SimpleBenchmark extends AbstractBenchmarkBase2 {
    @State(Scope.Thread)
    public static class BenchmarkState {
        Cluster cluster;
        SimpleClient simpleClient;
        Client client;
        SimpleClient skipClient;

        @Setup(Level.Trial)
        public void doSetup() throws Exception {
            cluster = Cluster.build("ec2-35-91-97-124.us-west-2.compute.amazonaws.com").port(45940).create();
            client = cluster.connect().alias("ggrateful");
            simpleClient = TestClientFactory.createWebSocketClient(URI.create("ws://ec2-35-91-97-124.us-west-2.compute.amazonaws.com:45940/gremlin"));
            skipClient = TestClientFactory.createWebSocketClientSkipDeser(URI.create("ws://ec2-35-91-97-124.us-west-2.compute.amazonaws.com:45940/gremlin"));
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws Exception {
            cluster.close();
            skipClient.close();
            simpleClient.close();
            client.close();
        }
    }


//    @Benchmark
    public List<Result> countGrateful(BenchmarkState state) throws Exception {
        final List<Result> res = state.client.submit("ggrateful.V().count();").all().get();
        return res;
    }

//    @Benchmark
    public Iterator<Result> both2Iterator(BenchmarkState state) throws Exception {
        Iterator<Result> res = state.client.submit("ggrateful.V().both().both();").iterator();
        while (res.hasNext()) {
            res.next();
        }
        return res;
    }

//    @Benchmark
    public List<Result> both2ToList(BenchmarkState state) throws Exception {
        List<Result> res = state.client.submit("ggrateful.V().both().both().toList();").all().get();

        return res;
    }


//    @Benchmark
    public List<ResponseMessage> simpleClient(BenchmarkState state) throws Exception {
        final List<ResponseMessage> res = state.simpleClient.submit("g.V().count()");
        return res;
        // 0.046s (slow iterations, doesn't exit)
    }

//    @Benchmark
    public List<ResponseMessage> countGratefulSimple(BenchmarkState state) throws Exception {
        final List<ResponseMessage> res = state.simpleClient.submit("ggrateful.V().count();");
        return res;
        // 0.047s (slow iterations, doesn't exit)
    }

//    @Benchmark
    public Iterator<ResponseMessage> both2IteratorSimple(BenchmarkState state) throws Exception {
        Iterator<ResponseMessage> res = state.simpleClient.submit("ggrateful.V().both().both();")
                .iterator();
        while (res.hasNext()) {
            res.next();
        }
        return res;
        // while: out of memory + hangs
        // no while: 5.7s warmup then out of memory + hangs
    }

//    @Benchmark
    public List<ResponseMessage> both2ToListSimple(BenchmarkState state) throws Exception {
        List<ResponseMessage> res = state.simpleClient.submit("ggrateful.V().both().both().toList();");
        return res;
        // 7s warmup
        // 12s it1
        // out of memory it2
    }


    @Benchmark
    public List<ResponseMessage> simpleSkipDeser(BenchmarkState state) throws Exception {
        final List<ResponseMessage> res = state.skipClient.submit("g.V().count()");
        return res;
        // 0.046s (slow iterations, doesn't exit)
    }

    public static void main(String[] args) throws Exception {
        SimpleClient skipClient = TestClientFactory.createWebSocketClientSkipDeser(URI.create("ws://ec2-35-91-97-124.us-west-2.compute.amazonaws.com:45940/gremlin"));
        final List<ResponseMessage> res = skipClient.submit("ggrateful.V().count();");
    }
    @Benchmark
    public List<ResponseMessage> countGratefulSkipDeser(BenchmarkState state) throws Exception {
        final List<ResponseMessage> res = state.skipClient.submit("ggrateful.V().count();");
        return res;
        // 0.047s (slow iterations, doesn't exit)
    }
}
