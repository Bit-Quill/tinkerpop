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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerTransactionGraph;
import org.apache.tinkerpop.gremlin.util.tools.CollectionFactory;

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
        //runThroughPutReadQuery(tinkerTraversal, numTimesToRepeatQuery);
        //runThroughPutReadQuery(txTraversal, numTimesToRepeatQuery);
        runLatencyCentralVertexDropTest(numTimesToRepeatQuery);
        runLatencyRandomVertexDropTest(numTimesToRepeatQuery);
        runLatencyRandomEdgeDropTest(numTimesToRepeatQuery);
        runLatencyRandomEdgeMoveTestV2(numTimesToRepeatQuery);
        runLatencyRandomElementPropertyUpdateTest(numTimesToRepeatQuery);
        runLatencyRandomVertexPropertyAddTest(numTimesToRepeatQuery);
        runLatencyRandomEdgePropertyUpsertTest(numTimesToRepeatQuery);
        runLatencySingleCommitAddTest(numTimesToRepeatQuery);
        //runThroughputCentralVertexDropTest(numTimesToRepeatQuery);
        //runThroughputRandomVertexDropTest(numTimesToRepeatQuery);
        runLatencyRollbackAddTest(numTimesToRepeatQuery);
        runLatencyRandomVertexDropRollbackTest(numTimesToRepeatQuery);
        runLatencyRandomEdgeMoveRollbackTestV2(numTimesToRepeatQuery);
        runAddDirectRouteForIndirectTest(numTimesToRepeatQuery);
        runReduceAirportTrafficTest(numTimesToRepeatQuery);
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

        try {
            txGraph.io(graphml()).readGraph("air-routes-latest.graphml");
            Commit(txGraph.traversal());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to load air-routes");
        }

        return txGraph.traversal();
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

            TinkerTransactionGraph tg = TinkerTransactionGraph.open(getTinkerGraphConf());
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

            TinkerTransactionGraph tg = TinkerTransactionGraph.open(getTinkerGraphConf());
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

    public static void runThroughputCentralVertexDropTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vIds = g.V().order().by(bothE().count(), desc).id().toList();

            ExecutorService threadPool = Executors.newFixedThreadPool(2);
            List<Future<?>> futures = new ArrayList<>();

            Long start = System.nanoTime();
            for (Object id : vIds) {
                futures.add(threadPool.submit(() -> {
                    g.V(id).drop().iterate();
                    Commit(g);
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

    public static void runThroughputRandomVertexDropTest(int numTimes) {
        for (; numTimes != 0; numTimes--) {
            GraphTraversalSource g = createAirRoutesTraversalSource();
            List<Object> vIds = g.V().id().toList();
            Random random = new Random(2);
            Collections.shuffle(vIds, random);

            ExecutorService threadPool = Executors.newFixedThreadPool(2);
            List<Future<?>> futures = new ArrayList<>();

            Long start = System.nanoTime();
            for (int i=0 ; i < vIds.size()/5; i++) {
                int index = i;
                futures.add(threadPool.submit(() -> {
                    g.V(vIds.get(index)).drop().iterate();
                    Commit(g);
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
                    g.mergeV(CollectionFactory.asMap(T.id, vId))
                            .option(Merge.onMatch, CollectionFactory.asMap("newprop", "new"))
                            .iterate();
                }
                Commit(g);
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly update vertices properties");
        }
    }

    public static void runThroughputRandomVertexPropertyAddTest(int numTimes) {
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
                    g.mergeV(CollectionFactory.asMap(T.id, vId))
                            .option(Merge.onMatch, CollectionFactory.asMap("newprop", "new"))
                            .iterate();
                }
                Commit(g);
            }

            Long end = System.nanoTime();

            cleanup(g);

            System.out.println("It took " + (end-start)/1000000 + " ms to randomly update vertices and edges");
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
                    g.mergeE(CollectionFactory.asMap(Direction.from, vertices.get(random.nextInt(numVertices)), Direction.to, vertices.get(random.nextInt(numVertices))))
                            .option(Merge.onCreate, CollectionFactory.asMap(vals[random.nextInt(numVals)], vals[random.nextInt(numVals)]))
                            .option(Merge.onMatch, CollectionFactory.asMap(vals[random.nextInt(numVals)], vals[random.nextInt(numVals)]))
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
    public static void runThroughPutReadQuery(GraphTraversalSource gts, int numTimes) {
        final int NUM_QUERIES = 100000;

        while (numTimes != 0) {
            ExecutorService jobPool = Executors.newFixedThreadPool(16);
            List<Future<GraphTraversal>> futures = new ArrayList<>(NUM_QUERIES);

            final Long start = System.nanoTime();
            for (int i = 0; i < NUM_QUERIES; i++) {
                futures.add(jobPool.submit(() -> gts.V().both().iterate()));
            }

            try {
                Thread.sleep(20000);
                Long end = System.nanoTime();
                while (end - start < 21000) {
                    Thread.sleep(10);
                    end = System.nanoTime();
                }
                jobPool.shutdownNow();
                System.out.println("Total Runtime is: " + (end - start) + " ms.");
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
            try {
                gts.getGraph().close();
            } catch (Exception e) {
                // Intentionally do nothing.
            }

            numTimes--;
        }
    }
    public static long traversalRunTime(GraphTraversal traversal) {
        long startTime = System.nanoTime();
        traversal.iterate();
        return (System.nanoTime() - startTime)/1000000;
    }
}
