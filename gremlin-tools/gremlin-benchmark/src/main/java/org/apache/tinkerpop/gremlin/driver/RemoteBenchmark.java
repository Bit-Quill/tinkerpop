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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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

import java.util.Iterator;
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
        GraphTraversalSource g;
        Cluster cluster;
        Client client;

        @Setup(Level.Trial)
        public void doSetup() throws Exception {
            cluster = Cluster.build("ec2-35-91-97-124.us-west-2.compute.amazonaws.com").port(45940).create();
            client = cluster.connect().alias("ggrateful");
            g = traversal().withRemote(DriverRemoteConnection.using(cluster, "ggrateful"));
            System.out.println("count: " + g.V().count().next());
        }


        @TearDown(Level.Trial)
        public void doTearDown() throws Exception {
            cluster.close();
            g.close();
        }
    }


    @Benchmark
    public GraphTraversal<Vertex, Vertex> both3(BenchmarkState state) throws Exception {
//        List<Vertex> res = state.g.V().repeat(both()).times(5).toList();
        GraphTraversal<Vertex, Vertex> res = state.g.V().both().both().both();
        while (res.hasNext()) {
            res.next();
        }
        return res;
        // 1s, 1.9s, 3.7s
    }

    @Benchmark
    public Long countBoths(BenchmarkState state) throws Exception {
        Long res = state.g.V().both().both().both().count().next();
        return res;
        // 0.047s
    }

    @Benchmark
    public List<Result> countGrateful(BenchmarkState state) throws Exception {
        final List<Result> res = state.client.submit("ggrateful.V().count();").all().get();
        return res;
        // 0.044s
    }

    @Benchmark
    public Iterator<Result> both2Iterator(BenchmarkState state) throws Exception {
        Iterator<Result> res = state.client.submit("ggrateful.V().both().both();").iterator();
        while (res.hasNext()) {
            res.next();
        }
        return res;
        // 5.7s
    }

    @Benchmark
    public List<Result> both2ToList(BenchmarkState state) throws Exception {
        List<Result> res = state.client.submit("ggrateful.V().both().both().toList();").all().get();
        return res;
        // 5.8s
    }
}
