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
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
@Warmup(time = 2001, timeUnit = MILLISECONDS, iterations = 1)
@Measurement(time = 2002, timeUnit = MILLISECONDS, iterations = 2)
@BenchmarkMode(Mode.AverageTime)
public class RemoteBenchmark extends AbstractBenchmarkBase2 {
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
            cluster = Cluster.build("ec2-35-91-97-124.us-west-2.compute.amazonaws.com").port(45940).create();
            g = traversal().withRemote(DriverRemoteConnection.using(cluster, "ggrateful"));
//            g.V().drop().iterate();
//            for (int i = 0; i < 5000000; i
//                    ++) {
//                g.addV("person").property("name","vadas").next();
                System.out.println("count: " + g.V().count().next());
//            }
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
        public void doTearDown() throws Exception {
//            System.out.println("count before: " + g.V().count().next());
//            g.V().drop().iterate();
            System.out.println("count after: " + g.V().count().next());
            cluster.close();
            g.close();
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

//    @Benchmark
    public GraphTraversal<Vertex, Vertex> limit1k(BenchmarkState state) throws Exception {
        GraphTraversal<Vertex, Vertex> res = state.g.V().limit(1000);
        state.j += 1;
//        System.out.println(state.j);
        return res;
    }

    @Benchmark
    public GraphTraversal<Vertex, Vertex> both5(BenchmarkState state) throws Exception {
//        List<Vertex> res = state.g.V().repeat(both()).times(5).toList();
        GraphTraversal<Vertex, Vertex> res = state.g.V().both().both().both();
        while (res.hasNext()) {
            res.next();
        }
        state.j += 1;
//        System.out.println(state.j);
        return res;
    }

//    @Benchmark
    public Long countBoths(BenchmarkState state) throws Exception {


//        List<Vertex> res = state.g.V().repeat(both()).times(5).toList();
        Long res = state.g.V().both().both().both().count().next();
        state.j += 1;
//        System.out.println(state.j);
        return res;
    }
}
