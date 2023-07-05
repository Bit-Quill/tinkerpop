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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(GremlinProcessRunner.class)
public abstract class ConcatTest extends AbstractGremlinProcessTest {

    public abstract Traversal<String, String> get_g_injectXnull_a_b_nullX_concat();
    public abstract Traversal<Vertex, String> get_g_hasLabelXpersonX_valuesXnameX_asXaX_constantXMrX_concatXselect_aX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_injectXnull_a_b_nullX_concat() {
        final Traversal<String, String> traversal = get_g_injectXnull_a_b_nullX_concat();
        printTraversalForm(traversal);
        String concatenated = traversal.next();
        assertEquals("a", concatenated);
        assertTrue(traversal.hasNext());
        concatenated = traversal.next();
        // TODO - null will be returned as emtpy if traverser isn't all null
        assertEquals("", concatenated);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_hasLabelXpersonX_valuesXnameX_asXaX_constantXMrX_concatXselect_aX() {
        final Traversal<Vertex, String> traversal = get_g_hasLabelXpersonX_valuesXnameX_asXaX_constantXMrX_concatXselect_aX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("Mr.marko", "Mr.vadas", "Mr.josh", "Mr.peter"), traversal);
    }


    public static class Traversals extends ConcatTest {
        @Override
        public Traversal<String, String> get_g_injectXnull_a_b_nullX_concat() {
            return g.inject("a", null).concat();
        }

        @Override
        public Traversal<Vertex, String> get_g_hasLabelXpersonX_valuesXnameX_asXaX_constantXMrX_concatXselect_aX() {
            return g.V().hasLabel("person").values("name").as("a").constant("Mr.").concat(__.select("a"));
        }

    }

}
