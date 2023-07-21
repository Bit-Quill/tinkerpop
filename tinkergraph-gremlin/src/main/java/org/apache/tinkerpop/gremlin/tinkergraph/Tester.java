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
package org.apache.tinkerpop.gremlin.tinkergraph;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGenerator;
import org.apache.tinkerpop.gremlin.algorithm.generator.PowerLawDistribution;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerTransactionGraph;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph.DefaultIdManager.LONG;

public class Tester {
    public static final int GRAPH_TINKER = 0;
    public static final int GRAPH_TXN_TINKER = 1;
    public static final int GRAPH_NEO4J = 2;

    public static int GRAPH_TYPE;

    public static void main(String[] args) {
        if (args.length < 2) throw new IllegalArgumentException("Test expected more parameters");

        int numTimesToRepeatQuery = Integer.parseInt(args[0]);
        GRAPH_TYPE = Integer.parseInt(args[1]);

        System.out.println("Running test v1 for " + numTimesToRepeatQuery + " times and type " + GRAPH_TYPE);

        runLatencyReadQuery(numTimesToRepeatQuery);
        runLatencyCentralVertexDropTest(numTimesToRepeatQuery);
        runLatencyRandomVertexDropTest(numTimesToRepeatQuery);
        runLatencyRandomEdgeDropTest(numTimesToRepeatQuery);
        runLatencyRandomEdgeMoveTestV2(numTimesToRepeatQuery);
        runLatencyRandomElementPropertyUpdateTest(numTimesToRepeatQuery);
        runLatencyRandomVertexPropertyAddTest(numTimesToRepeatQuery);
        runLatencyRandomEdgePropertyUpsertTest(numTimesToRepeatQuery);
        runLatencySingleCommitAddTest(numTimesToRepeatQuery);
        runLatencyRollbackAddTest(numTimesToRepeatQuery);
        runLatencyRandomVertexDropRollbackTest(numTimesToRepeatQuery);
        runLatencyRandomEdgeMoveRollbackTestV2(numTimesToRepeatQuery);
        runAddDirectRouteForIndirectTest(numTimesToRepeatQuery);
        runReduceAirportTrafficTest(numTimesToRepeatQuery);

        runThroughPutReadQuery(numTimesToRepeatQuery);
        runThroughputAddDropVertexTest(numTimesToRepeatQuery);
        runThroughputAddEdgeTest(numTimesToRepeatQuery);
        runThroughputAddVertexPropertyTest(numTimesToRepeatQuery);
        runThroughputAddEdgePropertyTest(numTimesToRepeatQuery);

        //runFunctionalAddRemoveVertexTest();
        //runFunctionalAddRemoveVertexRollbackTest();
        //runFunctionalAddRemoveEdgeTest();
        //runFunctionalUpdateVertexPropertyTest();
        //runFunctionalRollbackVertexPropertyTest();
        //runFunctionalMergeVertexPropertyTest();
        //runFunctionalUpdateEdgePropertyTest();
        //runFunctionalRollbackEdgePropertyTest();
        //runFunctionalDropVertexTest();
        //runFunctionalDropCentralVertexTest();
    }

    private static Configuration getTinkerGraphConf() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, LONG);
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, LONG);
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, LONG);
        return conf;
    }

    public static void Commit(GraphTraversalSource g) {
        if ( ! (GRAPH_TYPE == GRAPH_TINKER)) {
            g.tx().commit();
        }
    }

    public static void Rollback(GraphTraversalSource g) {
        if ( ! (GRAPH_TYPE == GRAPH_TINKER)) {
            g.tx().rollback();
        }
    }
    public static GraphTraversalSource createAirRoutesTraversalSource() {

        Graph txGraph = createGraph();

        try {
            txGraph.io(graphml()).readGraph("air-routes-latest.graphml");
            Commit(txGraph.traversal());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to load air-routes");
        }

        return txGraph.traversal();
    }

    public static Graph createGraph() {
        Graph txGraph;
        if (GRAPH_TYPE == GRAPH_TINKER) {
            txGraph = TinkerGraph.open(getTinkerGraphConf());
        } else if (GRAPH_TYPE == GRAPH_TXN_TINKER) {
            txGraph = TinkerTransactionGraph.open(getTinkerGraphConf());
        } else if (GRAPH_TYPE == GRAPH_NEO4J) {
            txGraph = Neo4jGraph.open("/tmp/neo4j/" + UUID.randomUUID());
        } else {
            throw new RuntimeException("GRAPH_TYPE " + GRAPH_TYPE + " unsupported.");
        }

        return txGraph;
    }

    public static void cleanup(GraphTraversalSource g) {
        if (GRAPH_TYPE == GRAPH_TXN_TINKER || GRAPH_TYPE == GRAPH_NEO4J) {
            try {
                g.getGraph().close();
            } catch (Exception e) {
                // Intentionally blank
            }
        }
    }

    public static void runAddDirectRouteForIndirectTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {

            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vIds = g.V().id().toList();
            Long start = System.nanoTime();

            // TODO: this takes too long to run. Maybe reduce to 300 vertices.
            for (int i = 0; i < 800; i++) {
                g.V(vIds.get(i)).as("start")
                        .in().out().as("end")
                        .where("end", neq("start"))
                        .where(out().is(neq("start")))
                        .addE("route").to("start")
                        .iterate();
                Commit(g);
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " to add extra direct flights");
        }
    }

    public static void runReduceAirportTrafficTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {

            GraphTraversalSource b = TinkerFactory.createModern().traversal();
            GraphTraversalSource g = createAirRoutesTraversalSource();
            Random random = new Random(5);
            Long start = System.nanoTime();

            for (int i = 0; i < 1000; i++) {
                for (Vertex v : g.V().hasLabel("airport").order().by(__.both().count(), desc).limit(2).toList()) {
                    for (Edge e : g.V(v.id()).outE().toList()) {
                        if (random.nextInt(3) == 0) {
                            g.E(e.id()).drop().iterate();
                        }
                    }
                }
                Commit(g);
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " to remove overloaded routes");
        }
    }

    public static void runLatencySingleCommitAddTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {

            Graph tg = createGraph();
            GraphTraversalSource g = tg.traversal();
            Long start = System.nanoTime();
            for (int i=0; i<1000; i++) {
                g.addV().iterate();
            }
            Commit(g);

            List<Vertex> vertices = g.V().toList();

            DistributionGenerator
                    .build(tg)
                    .label("knows")
                    .inDistribution(new PowerLawDistribution(2.5))
                    .outDistribution(new PowerLawDistribution(2.5))
                    .expectedNumEdges(500000)
                    .inVertices(vertices.subList(0, vertices.size()/2))
                    .outVertices(vertices.subList(vertices.size()/2, vertices.size()))
                    .create().generate();
            Commit(g);
            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " to create a power law 2.5 distributed graph");
        }
    }

    public static void runLatencyRollbackAddTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {

            Graph tg = createGraph();
            GraphTraversalSource g = tg.traversal();
            Long start = System.nanoTime();
            for (int i=0; i<1000; i++) {
                g.addV().iterate();
            }

            List<Vertex> vertices = g.V().toList();

            DistributionGenerator
                    .build(tg)
                    .label("knows")
                    .inDistribution(new PowerLawDistribution(2.5))
                    .outDistribution(new PowerLawDistribution(2.5))
                    .expectedNumEdges(500000)
                    .inVertices(vertices.subList(0, vertices.size()/2))
                    .outVertices(vertices.subList(vertices.size()/2, vertices.size()))
                    .create().generate();

            Rollback(g);

            Long end = System.nanoTime();
            System.out.println("It took " + (end-start)/1000000 + " to rollback a power law 2.5 distributed graph");

            cleanup(g);
        }
    }

    public static void runLatencyCentralVertexDropTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vIds = g.V().order().by(bothE().count(), desc).id().toList();

            Long start = System.nanoTime();
            for (Object id : vIds) {
                g.V(id).drop().iterate();
                Commit(g);
            }
            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to drop all vertices");
        }
    }

    public static void runThroughputAddDropVertexTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createGraph().traversal();

            ExecutorService threadPool = Executors.newFixedThreadPool(6);
            List<Future<?>> futures = new ArrayList<>();

            Long start = System.nanoTime();
            for (int numMaxQueries=0; numMaxQueries < 1000000; numMaxQueries++) {
                futures.add(threadPool.submit(() -> {
                    boolean completed = false;
                    for (int j = 0; j < 10000; j++) {
                        try {
                            for (int k=0; k<5; k++) {
                                g.addV().property("someval", "someotherVal").iterate();
                            }
                            g.V().drop().iterate();
                            Commit(g);
                            completed = true;
                            break;
                        } catch (TransactionException te) {
                            g.tx().rollback();
                        }
                    }
                    if (completed == false) System.out.println("not completed");
                }));
            }

            try {
                Thread.sleep(5000);
                threadPool.shutdownNow();
                Long end = System.nanoTime();
                System.out.println("Total Runtime is: " + (end - start)/1000000 + " ms.");
            } catch (Exception e) {
                e.printStackTrace();
            }

            long numDoneNormally = 0;
            for (Future<?> future : futures.stream().filter(Future::isDone).collect(Collectors.toList())) {
                try {
                    future.get();
                    numDoneNormally++;
                } catch (Exception e) {
                    if (!(e.getCause() instanceof TraversalInterruptedException)) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Completed " + numDoneNormally + " add and drop vertex queries.");

            cleanup(g);
        }
    }

    public static void runThroughputAddEdgeTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createGraph().traversal();
            for (int numVerticesToAdd=0; numVerticesToAdd<1000; numVerticesToAdd++) {
                g.addV().iterate();
            }
            Commit(g);
            List<Vertex> vertices = g.V().toList();
            int numVertices = vertices.size();

            ExecutorService threadPool = Executors.newFixedThreadPool(6);
            List<Future<?>> futures = new ArrayList<>();
            Random random = new Random(19);

            Long start = System.nanoTime();
            for (int numMaxQueries=0; numMaxQueries < 1000000; numMaxQueries++) {
                futures.add(threadPool.submit(() -> {
                    try {
                        g.addE("edge").from(vertices.get(random.nextInt(numVertices))).to(vertices.get(random.nextInt(numVertices))).iterate();
                        Commit(g);
                    } catch (TransactionException te) {
                        g.tx().rollback();
                    }
                }));
            }

            try {
                Thread.sleep(5000);
                threadPool.shutdownNow();
                Long end = System.nanoTime();
                System.out.println("Total Runtime is: " + (end - start)/1000000 + " ms.");
            } catch (Exception e) {
                e.printStackTrace();
            }

            long numDoneNormally = 0;
            for (Future<?> future : futures.stream().filter(Future::isDone).collect(Collectors.toList())) {
                try {
                    future.get();
                    numDoneNormally++;
                } catch (Exception e) {
                    if (!(e.getCause() instanceof TraversalInterruptedException)) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Completed " + numDoneNormally + " add and drop edge queries.");

            cleanup(g);
        }
    }

    public static void runThroughputAddVertexPropertyTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createGraph().traversal();
            Object vId = g.addV().id().next();
            Commit(g);

            ExecutorService threadPool = Executors.newFixedThreadPool(6);
            List<Future<?>> futures = new ArrayList<>();
            Random random = new Random(19);

            Long start = System.nanoTime();
            for (int numMaxQueries=0; numMaxQueries < 3000000; numMaxQueries++) {
                String propKey = String.valueOf(numMaxQueries);
                futures.add(threadPool.submit(() -> {
                    try {
                        g.V(vId).property(propKey, "abc").iterate();
                        Commit(g);
                    } catch (TransactionException te) {
                        g.tx().rollback();
                    }
                }));
            }

            try {
                Thread.sleep(5000);
                threadPool.shutdownNow();
                Long end = System.nanoTime();
                System.out.println("Total Runtime is: " + (end - start)/1000000 + " ms.");
            } catch (Exception e) {
                e.printStackTrace();
            }

            long numDoneNormally = 0;
            for (Future<?> future : futures.stream().filter(Future::isDone).collect(Collectors.toList())) {
                try {
                    future.get();
                    numDoneNormally++;
                } catch (Exception e) {
                    if (!(e.getCause() instanceof TraversalInterruptedException)) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Completed " + numDoneNormally + " add vertex PROPERTY queries.");

            cleanup(g);
        }
    }

    public static void runThroughputAddEdgePropertyTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createGraph().traversal();
            Vertex v1 = g.addV().next();
            Vertex v2 = g.addV().next();
            Edge e1 = g.addE("edge").from(v1).to(v2).next();
            Object eId = e1.id();
            Commit(g);

            ExecutorService threadPool = Executors.newFixedThreadPool(6);
            List<Future<?>> futures = new ArrayList<>();

            Long start = System.nanoTime();
            for (int numMaxQueries=0; numMaxQueries < 3000000; numMaxQueries++) {
                String propKey = String.valueOf(numMaxQueries);
                futures.add(threadPool.submit(() -> {
                    try {
                        g.E(eId).property(propKey, "abc").iterate();
                        Commit(g);
                    } catch (TransactionException te) {
                        g.tx().rollback();
                    }
                }));
            }

            try {
                Thread.sleep(5000);
                threadPool.shutdownNow();
                Long end = System.nanoTime();
                System.out.println("Total Runtime is: " + (end - start)/1000000 + " ms.");
            } catch (Exception e) {
                e.printStackTrace();
            }

            long numDoneNormally = 0;
            for (Future<?> future : futures.stream().filter(Future::isDone).collect(Collectors.toList())) {
                try {
                    future.get();
                    numDoneNormally++;
                } catch (Exception e) {
                    if (!(e.getCause() instanceof TraversalInterruptedException)) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Completed " + numDoneNormally + " add edge PROPERTY queries.");

            cleanup(g);
        }
    }

    public static void runLatencyRandomVertexDropTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vIds = g.V().order().by(id(), desc).id().toList();
            Random random = new Random(5);
            Collections.shuffle(vIds, random);

            Long start = System.nanoTime();
            for (Object id : vIds) {
                g.V(id).drop().iterate();
                Commit(g);
            }
            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly drop all vertices");
        }
    }

    public static void runLatencyRandomVertexDropRollbackTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vIds = g.V().order().by(id(), desc).id().toList();
            Random random = new Random(5);
            Collections.shuffle(vIds, random);

            Long start = System.nanoTime();
            for (Object id : vIds) {
                g.V(id).drop().iterate();
                Rollback(g);
            }
            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly rollback all dropped vertices");
        }
    }


    public static void runThroughputAddVertexTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();

            ExecutorService threadPool = Executors.newFixedThreadPool(6);
            List<Future<?>> futures = new ArrayList<>();

            Long numVertices = g.V().count().next();

            final int numVerticesToModify = 5;
            Long start = System.nanoTime();
            for (; numVertices > numVerticesToModify; numVertices-=numVerticesToModify) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 10000; j++) {
                        try {
                            List<Object> ids = g.V().limit(numVerticesToModify).id().toList();
                            for (int k = 0; k < numVerticesToModify; k++) {
                                g.V(ids.get(k)).drop().iterate();
                            }
                            Commit(g);
                            if (j != 0) System.out.println("j is " + j);
                            break;
                        } catch (TransactionException te) {
                            g.tx().rollback();
                        }
                    }
                }));
            }

            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            Long end = System.nanoTime();
            System.out.println("It took " + (end-start)/1000000 + " ms to drop all vertices");

            cleanup(g);

            threadPool.shutdown();
        }
    }

    public static void runLatencyRandomEdgeDropTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> edgeIds = g.E().order().by(id(), desc).id().toList();
            Random random = new Random(5);
            Collections.shuffle(edgeIds, random);

            Long start = System.nanoTime();
            for (Object id : edgeIds) {
                g.E(id).drop().iterate();
                Commit(g);
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly drop all edges");
        }
    }

    // This test is incompatible with Neo4j. Throws IAE class org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jProperty is not supported
    public static void runLatencyRandomEdgeMoveTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vertexIds = g.V().id().toList();
            List<Map<Object,Object>> edgeElements = (List) g.E().project("id", "label", "props")
                    .by(id()).by(label()).by(propertyMap()).toList();
            Random random = new Random(5);
            Collections.shuffle(edgeElements, random);

            final int numVertices = vertexIds.size();
            Long start = System.nanoTime();
            for (Map<Object, Object> edgeElement : edgeElements) {
                int fromVertexId = random.nextInt(numVertices);
                int toVertexId = random.nextInt(numVertices);

                g.V(toVertexId).addE((String) edgeElement.get("label")).from(V(fromVertexId)).property((Map<Object, Object>) edgeElement.get("props")).iterate();
                g.E(edgeElement.get("id")).drop().iterate();
                Commit(g);
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly move all edges");
        }
    }

    public static void runLatencyRandomEdgeMoveTestV2(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vertexIds = g.V().id().toList();
            Long numEdges = g.E().count().next();
            Random random = new Random(5);

            final int numVertices = vertexIds.size();
            Long start = System.nanoTime();
            for (int i = 0; i < numEdges; i++) {
                int fromVertexId = random.nextInt(numVertices);
                int toVertexId = random.nextInt(numVertices);

                g.V(fromVertexId).as("v1")
                        .outE().limit(1).as("e1")
                        .V(toVertexId).addE("relatesTo").from("v1").as("e2")
                        .sideEffect(select("e1").properties().unfold().as("p1").select("e2").property(select("p1").key(), select("p1").value()))
                        .select("e1").drop().iterate();
                Commit(g);
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly move all edges");
        }
    }

    // This test is incompatible with Neo4j. Throws IAE class org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jProperty is not supported
    public static void runLatencyRandomEdgeMoveRollbackTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vertexIds = g.V().id().toList();
            List<Map<Object,Object>> edgeElements = (List) g.E().project("id", "label", "props")
                    .by(id()).by(label()).by(propertyMap()).toList();
            Random random = new Random(5);
            Collections.shuffle(edgeElements, random);

            final int numVertices = vertexIds.size();
            final int numEdgeElements = edgeElements.size();
            Long start = System.nanoTime();
            for (int i = 0; i < numEdgeElements; i++) {
                int fromVertexId = random.nextInt(numVertices);
                int toVertexId = random.nextInt(numVertices);

                g.V(toVertexId).addE((String) edgeElements.get(i).get("label")).from(V(fromVertexId)).property((Map<Object, Object>) edgeElements.get(i).get("props")).iterate();
                g.E(edgeElements.get(i).get("id")).drop().iterate();

                if (i % 2 != 0) {
                    Commit(g);
                } else {
                    Rollback(g);
                }
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly move half of the edges.");
        }
    }

    public static void runLatencyRandomEdgeMoveRollbackTestV2(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vertexIds = g.V().id().toList();
            final long numEdgeElements  = g.E().count().next();
            Random random = new Random(5);

            final int numVertices = vertexIds.size();
            Long start = System.nanoTime();
            for (int i = 0; i < numEdgeElements; i++) {
                int fromVertexId = random.nextInt(numVertices);
                int toVertexId = random.nextInt(numVertices);

                g.V(fromVertexId).as("v1")
                        .outE().limit(1).as("e1")
                        .V(toVertexId).addE("relatesTo").from("v1").as("e2")
                        .sideEffect(select("e1").properties().unfold().as("p1").select("e2").property(select("p1").key(), select("p1").value()))
                        .select("e1").drop().iterate();

                if (i % 2 != 0) {
                    Commit(g);
                } else {
                    Rollback(g);
                }
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly move half of the edges.");
        }
    }

    public static void runLatencyRandomElementPropertyUpdateTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vertices = g.V().id().toList();
            List<Object> edges = g.E().id().toList();
            Random random = new Random(5);
            Collections.shuffle(vertices, random);
            Collections.shuffle(edges, random);

            final int numVertices = vertices.size();
            final int numEdges = edges.size();
            SeedStrategy seedStrategy = new SeedStrategy(5);
            Long start = System.nanoTime();
            for (int i = 0; i < numEdges; i++) {
                int numVerticesToModify = random.nextInt(10);
                for (int j = 0; j < numVerticesToModify; j++) {
                    Object vId = vertices.get(random.nextInt(numVertices));
                    g.withStrategies(seedStrategy).V(vId).property(__.V(vId).properties().sample(1).key(), "changed").iterate();
                }

                int numEdgesToModify = random.nextInt(10);
                for (int k = 0; k < numEdgesToModify; k++) {
                    Object eId = edges.get(random.nextInt(numVertices));
                    g.withStrategies(seedStrategy)
                            .E(eId)
                            .property(__.coalesce(__.E(eId).properties().sample(1).key(), __.constant("newprop")), "changed")
                            .iterate();
                }
                Commit(g);
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly update vertices and edges");
        }
    }

    public static void runLatencyRandomVertexPropertyAddTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vertices = g.V().id().toList();
            Random random = new Random(5);
            Collections.shuffle(vertices, random);

            final int numVertices = vertices.size();
            final int halfNumVertices = numVertices/2;
            Long start = System.nanoTime();
            for (int i = 0; i < halfNumVertices; i++) {
                int numVerticesToModify = random.nextInt(3);
                for (int j = 0; j < numVerticesToModify; j++) {
                    Object vId = vertices.get(random.nextInt(numVertices));
                    g.mergeV(CollectionUtil.asMap(T.id, vId))
                            .option(Merge.onMatch, CollectionUtil.asMap("newprop", "new"))
                            .iterate();
                }
                Commit(g);
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly update vertices properties");
        }
    }

    public static void runLatencyRandomEdgePropertyUpsertTest(int numTimes) {
        String[] vals = new String[] {"ab", "bc", "cd", "de", "ef", "yz", "ac", "ae", "xy", "tv", "vt", "mk", "km", "mi"};
        final int numVals = vals.length;

        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vertices = g.V().id().toList();
            Random random = new Random(5);

            final int numVertices = vertices.size();
            Long start = System.nanoTime();
            for (int i = 0; i < 100000; i++) {
                    g.mergeE(CollectionUtil.asMap(Direction.from, vertices.get(random.nextInt(numVertices)), Direction.to, vertices.get(random.nextInt(numVertices))))
                            .option(Merge.onCreate, CollectionUtil.asMap(vals[random.nextInt(numVals)], vals[random.nextInt(numVals)]))
                            .option(Merge.onMatch, CollectionUtil.asMap(vals[random.nextInt(numVals)], vals[random.nextInt(numVals)]))
                            .iterate();
                Commit(g);
            }

            Long end = System.nanoTime();
            System.out.println("It took " + (end-start)/1000000 + " ms to randomly upsert edges");

            cleanup(g);
        }
    }

    public static void runLatencyReadQuery(int numTimes) {
        while (numTimes != 0) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            GraphTraversal query = g.V().repeat(both()).times(7);
            System.out.println("It took " + traversalRunTime(query) + " ms to run for " + g.getGraph());
            cleanup(g);
            numTimes--;
        }
    }
    public static void runThroughPutReadQuery(int numTimes) {
        final int NUM_QUERIES = 100000;
        GraphTraversalSource gts = createAirRoutesTraversalSource();

        while (numTimes != 0) {
            ExecutorService jobPool = Executors.newFixedThreadPool(16);
            List<Future<GraphTraversal>> futures = new ArrayList<>(NUM_QUERIES);

            final Long start = System.nanoTime();
            for (int i = 0; i < NUM_QUERIES; i++) {
                futures.add(jobPool.submit(() -> gts.V().both().iterate()));
            }

            try {
                Thread.sleep(10000);
                jobPool.shutdownNow();
                Long end = System.nanoTime();
                System.out.println("Total Runtime is: " + (end - start)/1000000 + " ms.");
            } catch (Exception e) {
                e.printStackTrace();
            }

            long numDoneNormally = 0;
            for (Future<GraphTraversal> future : futures.stream().filter(Future::isDone).collect(Collectors.toList())) {
                try {
                    future.get();
                    numDoneNormally++;
                } catch (Exception e) {
                    if (!(e.getCause() instanceof TraversalInterruptedException)) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Completed " + numDoneNormally + " queries.");


            numTimes--;
        }

        cleanup(gts);
    }
    public static long traversalRunTime(GraphTraversal traversal) {
        long startTime = System.nanoTime();
        traversal.iterate();
        return (System.nanoTime() - startTime)/1000000;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  FUNCTIONAL TESTS //////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void runFunctionalAddRemoveVertexTest() {
        ExecutorService threadPool = Executors.newFixedThreadPool(12);
        List<Future<?>> futures = new ArrayList<>();

        GraphTraversalSource g = createGraph().traversal();
        Commit(g);

        for (int h=0; h<1000; h++) {
            for (int i = 0; i < 100; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 10000; j++) {
                        try {
                            g.addV().property(T.id, 5).iterate();
                            g.V(5).drop().iterate();
                            Commit(g);
                            break;
                        } catch (TransactionException|IllegalArgumentException te) {
                            g.tx().rollback();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (g.V().count().next() != 0) {
                System.out.println("Expected 0 count");
            }

        }
        cleanup(g);
        threadPool.shutdown();
    }

    public static void runFunctionalAddRemoveVertexRollbackTest() {
        ExecutorService threadPool = Executors.newFixedThreadPool(12);
        List<Future<?>> futures = new ArrayList<>();

        GraphTraversalSource g = createGraph().traversal();
        Commit(g);

        for (int h=0; h<10000; h++) {
            for (int i = 0; i < 100; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 10000; j++) {
                        try {
                            g.addV().property(T.id, 5).iterate();
                            g.V(5).drop().iterate();
                            Rollback(g);
                            break;
                        } catch (TransactionException|IllegalArgumentException te) {
                            g.tx().rollback();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (g.V().count().next() != 0) {
                System.out.println("Expected 0 count");
            }

        }
        cleanup(g);
        threadPool.shutdown();
    }

    public static void runFunctionalAddRemoveEdgeTest() {
        ExecutorService threadPool = Executors.newFixedThreadPool(12);
        List<Future<?>> futures = new ArrayList<>();

        GraphTraversalSource g = createGraph().traversal();
        Vertex v = g.addV().next();
        Commit(g);

        for (int h=0; h<1000; h++) {
            for (int i = 0; i < 100; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 10000; j++) {
                        try {
                            g.addE("e").property(T.id, 5).from(v).to(v).iterate();
                            g.E(5).drop().iterate();
                            Commit(g);
                            break;
                        } catch (TransactionException|IllegalArgumentException te) {
                            g.tx().rollback();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (g.E().count().next() != 0) {
                System.out.println("Expected 0 count");
            }

        }
        cleanup(g);
        threadPool.shutdown();
    }

    public static void runFunctionalUpdateVertexPropertyTest() {
        List<Map<Object, Object>> properties = new ArrayList<>();
        properties.add(CollectionUtil.asMap("a", "1", "b", "1", "c", "1", "d", "1", "e", "1"));
        properties.add(CollectionUtil.asMap("a", "2", "b", "2", "c", "2", "d", "2", "e", "2"));
        properties.add(CollectionUtil.asMap("a", "3", "b", "3", "c", "3", "d", "3", "e", "3"));
        properties.add(CollectionUtil.asMap("a", "4", "b", "4", "c", "4", "d", "4", "e", "4"));
        properties.add(CollectionUtil.asMap("a", "5", "b", "5", "c", "5", "d", "5", "e", "5"));

        Random random = new Random(5);
        ExecutorService threadPool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();

        GraphTraversalSource g = createGraph().traversal();
        Vertex v = g.addV().next();
        Commit(g);
        Object vId = v.id();

        for (int h=0; h<1000; h++) {
            for (int i = 0; i < 100; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 10000; j++) {
                        try {
                            g.V(vId).property(properties.get(random.nextInt(5))).iterate();
                            Commit(g);
                            break;
                        } catch (TransactionException te) {
                            g.tx().rollback();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            Vertex finalV = g.V().next();
            Iterator<VertexProperty<Object>> it = finalV.properties("a", "b", "c", "d", "e");
            String commonResult = it.next().value().toString();
            while (it.hasNext()) {
                Property<Object> result = it.next();
                if (result.value().toString() != commonResult) {
                    throw new RuntimeException("Expected " + commonResult + " but got " + result.value());
                }
            }
        }
        cleanup(g);
        threadPool.shutdown();
    }


    public static void runFunctionalMergeVertexPropertyTest() {
        List<Map<Object, Object>> properties = new ArrayList<>();
        properties.add(CollectionUtil.asMap("a", "1", "b", "1", "c", "1", "d", "1", "e", "1"));
        properties.add(CollectionUtil.asMap("a", "2", "b", "2", "c", "2", "d", "2", "e", "2"));
        properties.add(CollectionUtil.asMap("a", "3", "b", "3", "c", "3", "d", "3", "e", "3"));
        properties.add(CollectionUtil.asMap("a", "4", "b", "4", "c", "4", "d", "4", "e", "4"));
        properties.add(CollectionUtil.asMap("a", "5", "b", "5", "c", "5", "d", "5", "e", "5"));

        Random random = new Random();
        ExecutorService threadPool = Executors.newFixedThreadPool(12);
        List<Future<?>> futures = new ArrayList<>();

        GraphTraversalSource g = createGraph().traversal();
        Vertex v = g.addV().next();
        Commit(g);
        Object vId = v.id();

        for (int h=0; h<1000; h++) {
            for (int i = 0; i < 100; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 10000; j++) {
                        try {
                            g.V(vId).property(properties.get(random.nextInt(5))).iterate();
                            g.mergeV(CollectionUtil.asMap(T.id, vId))
                                    .option(Merge.onMatch, properties.get(random.nextInt(5)))
                                    .iterate();
                            Commit(g);
                            break;
                        } catch (TransactionException te) {
                            g.tx().rollback();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            Vertex finalV = g.V().next();
            Iterator<VertexProperty<Object>> it = finalV.properties("a", "b", "c", "d", "e");
            String commonResult = it.next().value().toString();
            while (it.hasNext()) {
                Property<Object> result = it.next();
                if (result.value().toString() != commonResult) {
                    throw new RuntimeException("Expected " + commonResult + " but got " + result.value());
                }
            }
        }
        cleanup(g);
        threadPool.shutdown();
    }

    public static void runFunctionalRollbackVertexPropertyTest() {
        List<Map<Object, Object>> properties = new ArrayList<>();
        properties.add(CollectionUtil.asMap("a", "1", "b", "1", "c", "1", "d", "1", "e", "1"));
        properties.add(CollectionUtil.asMap("a", "2", "b", "2", "c", "2", "d", "2", "e", "2"));
        properties.add(CollectionUtil.asMap("a", "3", "b", "3", "c", "3", "d", "3", "e", "3"));
        properties.add(CollectionUtil.asMap("a", "4", "b", "4", "c", "4", "d", "4", "e", "4"));
        properties.add(CollectionUtil.asMap("a", "5", "b", "5", "c", "5", "d", "5", "e", "5"));

        Random random = new Random(5);
        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        List<Future<?>> futures = new ArrayList<>();

        GraphTraversalSource g = createGraph().traversal();
        Vertex v = g.addV().property(CollectionUtil.asMap("a", "7", "b", "7", "c", "7", "d", "7", "e", "7")).next();
        Commit(g);
        Object vId = v.id();

        for (int h=0; h<10000; h++) {
            for (int i = 0; i < 30; i++) {
                futures.add(threadPool.submit(() -> {
                    try {
                        g.V(vId).property(properties.get(random.nextInt(5))).iterate();
                        Rollback(g);
                    } catch (TransactionException te) {
                        g.tx().rollback();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }));
            }
            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            Vertex finalV = g.V().next();
            Iterator<VertexProperty<Object>> it = finalV.properties("a", "b", "c", "d", "e");
            String commonResult = "7";
            while (it.hasNext()) {
                Property<Object> result = it.next();
                if (result.value().toString() != commonResult) {
                    throw new RuntimeException("Expected " + commonResult + " but got " + result.value());
                }
            }
        }
        cleanup(g);
        threadPool.shutdown();
    }

    public static void runFunctionalUpdateEdgePropertyTest() {
        List<Map<Object, Object>> properties = new ArrayList<>();
        properties.add(CollectionUtil.asMap("a", "1", "b", "1", "c", "1", "d", "1", "e", "1"));
        properties.add(CollectionUtil.asMap("a", "2", "b", "2", "c", "2", "d", "2", "e", "2"));
        properties.add(CollectionUtil.asMap("a", "3", "b", "3", "c", "3", "d", "3", "e", "3"));
        properties.add(CollectionUtil.asMap("a", "4", "b", "4", "c", "4", "d", "4", "e", "4"));
        properties.add(CollectionUtil.asMap("a", "5", "b", "5", "c", "5", "d", "5", "e", "5"));

        Random random = new Random(5);
        ExecutorService threadPool = Executors.newFixedThreadPool(12);
        List<Future<?>> futures = new ArrayList<>();

        GraphTraversalSource g = createGraph().traversal();
        g.addV().as("a").addV().as("b").addE("edge").from("a").to("b").iterate();
        Commit(g);

        Object eId = g.E().id().next();

        for (int h=0; h<1000; h++) {
            for (int i = 0; i < 50; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 10000; j++) {
                        try {
                            g.E(eId).as("e").property(properties.get(random.nextInt(5))).iterate();
                            Commit(g);
                            break;
                        } catch (TransactionException te) {
                            g.tx().rollback();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(100);
            } catch (Exception e) {

            }

            Edge e = g.E().next();
            Iterator<Property<Object>> it = e.properties("a", "b", "c", "d", "e");
            String commonResult = it.next().value().toString();
            while (it.hasNext()) {
                Property<Object> result = it.next();
                if (result.value().toString() != commonResult) {
                    throw new RuntimeException("Expected " + commonResult + " but got " + result.value());
                }
            }
        }
        cleanup(g);
        threadPool.shutdown();
    }

    public static void runFunctionalRollbackEdgePropertyTest() {
        List<Map<Object, Object>> properties = new ArrayList<>();
        properties.add(CollectionUtil.asMap("a", "1", "b", "1", "c", "1", "d", "1", "e", "1"));
        properties.add(CollectionUtil.asMap("a", "2", "b", "2", "c", "2", "d", "2", "e", "2"));
        properties.add(CollectionUtil.asMap("a", "3", "b", "3", "c", "3", "d", "3", "e", "3"));
        properties.add(CollectionUtil.asMap("a", "4", "b", "4", "c", "4", "d", "4", "e", "4"));
        properties.add(CollectionUtil.asMap("a", "5", "b", "5", "c", "5", "d", "5", "e", "5"));

        Random random = new Random(5);
        ExecutorService threadPool = Executors.newFixedThreadPool(12);
        List<Future<?>> futures = new ArrayList<>();

        GraphTraversalSource g = createGraph().traversal();
        g.addV().as("a").addV().as("b").addE("edge").from("a").to("b").iterate();
        g.E().property(CollectionUtil.asMap("a", "7", "b", "7", "c", "7", "d", "7", "e", "7")).iterate();
        Commit(g);

        Object eId = g.E().id().next();

        for (int h=0; h<1000; h++) {
            for (int i = 0; i < 50; i++) {
                futures.add(threadPool.submit(() -> {
                    try {
                        g.E(eId).as("e").property(properties.get(random.nextInt(5))).iterate();
                        Rollback(g);
                    } catch (TransactionException te) {
                        g.tx().rollback();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }));
            }
            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(100);
            } catch (Exception e) {

            }

            Edge e = g.E().next();
            Iterator<Property<Object>> it = e.properties("a", "b", "c", "d", "e");
            String commonResult = it.next().value().toString();
            while (it.hasNext()) {
                Property<Object> result = it.next();
                if (result.value().toString() != commonResult) {
                    System.out.println("Expected " + commonResult + " but got " + result.value());
                }
            }
        }
        cleanup(g);
        threadPool.shutdown();
    }

    public static void runFunctionalDropVertexTest() {
        ExecutorService threadPool = Executors.newFixedThreadPool(12);
        List<Future<?>> futures = new ArrayList<>();
        Random random = new Random(9);

        final int NUM_VERTICES_TO_KEEP = 1000;
        for (int h=0; h<10; h++) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vIds = g.V().id().toList();
            int numVertices = vIds.size();
            Collections.shuffle(vIds, random);

            List<Object> subsetOfVids = vIds.subList(numVertices - NUM_VERTICES_TO_KEEP, numVertices);
            List<Object> vIdsToKeep = new ArrayList<>(subsetOfVids);
            subsetOfVids.clear();
            numVertices = vIds.size();

            Set<Object> eIdsToKeep = new HashSet<>();
            for (int i=0; i<vIdsToKeep.size(); i++) {
                List<Edge> potentialEdgeToKeep = g.V(vIdsToKeep.get(i)).bothE().toList();
                for (Edge potentialE : potentialEdgeToKeep) {
                    if (vIdsToKeep.contains(potentialE.inVertex().id()) && vIdsToKeep.contains(potentialE.outVertex().id())) {
                        eIdsToKeep.add(potentialE.id());
                    }
                }
            }


            for (int i = 0; i < numVertices; i++) {
                int index = i;
                futures.add(threadPool.submit(() -> {
                    boolean finished = false;
                    for (int j = 0; j < 10000; j++) {
                        try {
                            g.V(vIds.get(index)).drop().iterate();
                            Commit(g);
                            finished = true;
                            break;
                        } catch (TransactionException te) {
                            g.tx().rollback();
                        }
                    }
                    if (!finished) System.out.println("not finished");
                }));
            }

            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(100);
            } catch (Exception e) {

            }

            List<Vertex> remainingVertices = g.V().toList();
            if (remainingVertices.size() != vIdsToKeep.size()) System.out.println("Size mismatch");
            for (Vertex v : remainingVertices) {
                if (!vIdsToKeep.contains(v.id())) System.out.println("Vertex not expected: " + v);
            }

            List<Object> remainingEdges = g.E().id().toList();
            if (remainingEdges.size() != eIdsToKeep.size()) System.out.println("Mismatched number of edges");
            System.out.println("remainingEdges size = " + remainingEdges.size());
            for (Object eId : remainingEdges) {
                if (!eIdsToKeep.contains(eId)) System.out.println("Edge not expected: " + eId);
            }

            cleanup(g);
        }

        threadPool.shutdown();
    }

    public static void runFunctionalDropCentralVertexTest() {
        ExecutorService threadPool = Executors.newFixedThreadPool(12);
        List<Future<?>> futures = new ArrayList<>();

        final int NUM_VERTICES_TO_KEEP = 1500;
        for (int h=0; h<10; h++) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vIds = g.V().order().by(bothE().count(), desc).id().toList();
            int numVertices = vIds.size();

            List<Object> subsetOfVids = vIds.subList(numVertices - NUM_VERTICES_TO_KEEP, numVertices);
            List<Object> vIdsToKeep = new ArrayList<>(subsetOfVids);
            subsetOfVids.clear();
            numVertices = vIds.size();

            Set<Object> eIdsToKeep = new HashSet<>();
            for (int i=0; i<vIdsToKeep.size(); i++) {
                List<Edge> potentialEdgeToKeep = g.V(vIdsToKeep.get(i)).bothE().toList();
                for (Edge potentialE : potentialEdgeToKeep) {
                    if (vIdsToKeep.contains(potentialE.inVertex().id()) && vIdsToKeep.contains(potentialE.outVertex().id())) {
                        eIdsToKeep.add(potentialE.id());
                    }
                }
            }


            for (int i = 0; i < numVertices; i++) {
                int index = i;
                futures.add(threadPool.submit(() -> {
                    boolean finished = false;
                    for (int j = 0; j < 10000; j++) {
                        try {
                            g.V(vIds.get(index)).drop().iterate();
                            Commit(g);
                            finished = true;
                            break;
                        } catch (TransactionException te) {
                            g.tx().rollback();
                        }
                    }
                    if (!finished) System.out.println("not finished");
                }));
            }

            while (futures.size() != 0) {
                List<Future<?>> completed = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                try {
                    for (Future<?> future : completed) {
                        future.get();
                    }
                    futures.removeAll(completed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(100);
            } catch (Exception e) {

            }

            List<Vertex> remainingVertices = g.V().toList();
            if (remainingVertices.size() != vIdsToKeep.size()) System.out.println("Size mismatch");
            for (Vertex v : remainingVertices) {
                if (!vIdsToKeep.contains(v.id())) System.out.println("Vertex not expected: " + v);
            }

            List<Object> remainingEdges = g.E().id().toList();
            if (remainingEdges.size() != eIdsToKeep.size()) System.out.println("Mismatched number of edges");
            System.out.println("remainingEdges size = " + remainingEdges.size());
            for (Object eId : remainingEdges) {
                if (!eIdsToKeep.contains(eId)) System.out.println("Edge not expected: " + eId);
            }

            cleanup(g);
        }

        threadPool.shutdown();
    }
}
