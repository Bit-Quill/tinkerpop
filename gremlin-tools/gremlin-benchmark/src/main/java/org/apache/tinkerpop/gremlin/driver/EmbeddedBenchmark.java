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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
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

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;


//@Warmup(time = 2000, timeUnit = MILLISECONDS)
@Warmup(time = 2001, timeUnit = MILLISECONDS, iterations = 4)
@Measurement(time = 2002, timeUnit = MILLISECONDS, iterations = 4)
@BenchmarkMode(Mode.AverageTime)
public class EmbeddedBenchmark extends AbstractBenchmarkBase2 {
    @State(Scope.Thread)
    public static class BenchmarkState {
        // fields here
        Graph graph;
        GraphTraversalSource g;
        Cluster cluster;
        SimpleClient client;
        int j = 0;
        @Setup(Level.Trial)
        public void doSetup() throws Exception {
            //
//            final Cluster cluster = TestClientFactory.build().create();
//            final SimpleClient client = TestClientFactory.createWebSocketClient();
            //

            graph = TinkerGraph.open();
            g = traversal().withEmbedded(graph);
//            Vertex v1 = g.addV("person").property("name","marko").next();
//            Vertex v2 = g.addV("person").property("name","stephen").next();
//            Vertex v3 = g.addV("person").property("name","vadas").next();

            // Be sure to use a terminating step like next() or iterate() so that the traversal "executes"
            // Iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
//            g.V(v1).addE("knows").to(v2).property("weight",0.75).iterate();
//            g.V(v1).addE("knows").to(v3).property("weight",0.75).iterate();

            for (int i = 0; i < 1000000; i++) {
                g.addV("person").property("name","vadas").next();
            }
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
//            bytecodeBuffer1.readerIndex(0);
//            bytecodeBuffer2.readerIndex(0);
//            pBuffer1.readerIndex(0);
//            bufferWrite.readerIndex(0);
//            bufferWrite.writerIndex(0);
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
//            bytecodeBuffer1.release();
//            bytecodeBuffer2.release();
        }
    }

//    @Benchmark
    public Long countJ(BenchmarkState state) throws Exception {
        Long res = state.g.V().count().next();
        state.j += 1;
        System.out.println(state.j);
        return res;
    }

//    @Benchmark
    public List<ResponseMessage> simpleClient(BenchmarkState state) throws Exception {
        final List<ResponseMessage> res = state.client.submit("g.V().count()");
        return res;
    }

    @Benchmark
    public GraphTraversal<Vertex, Vertex> limit1k(BenchmarkState state) throws Exception {
        GraphTraversal<Vertex, Vertex> res = state.g.V().limit(1000);
        state.j += 1;
//        System.out.println(state.j);
        return res;
    }

    @Benchmark
    public GraphTraversal<Vertex, Vertex> limit1m(BenchmarkState state) throws Exception {
        GraphTraversal<Vertex, Vertex> res = state.g.V().limit(1000000);
        state.j += 1;
//        System.out.println(state.j);
        return res;
    }
}
