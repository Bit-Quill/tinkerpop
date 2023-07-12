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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class ConcatStep<S> extends ScalarMapStep<S, String> implements TraversalParent {

    private String traversalResult;
    private String stringArgsResult;

    private Traversal.Admin<S, String> concatTraversal;

    // flag used to propagate the null value through if all strings to be concatenated are null
    private boolean isNullTraverser = true;
    private boolean isNullTraversal = true;
    private boolean isNullString = true;

    public ConcatStep(final Traversal.Admin traversal, final String... concatStrings) {
        super(traversal);
        this.stringArgsResult = processStrings(concatStrings);
    }

    public ConcatStep(final Traversal.Admin traversal, final Traversal<S, String> concatTraversal) {
        super(traversal);
        this.concatTraversal = this.integrateChild(concatTraversal.asAdmin());
        this.traversalResult = processTraversal(this.concatTraversal);
    }

    @Override
    protected String map(final Traverser.Admin<S> traverser) {
        // throws when incoming traverser isn't a string
        if (null != traverser.get() && !(traverser.get() instanceof String)) {
            throw new IllegalArgumentException(
                    String.format("String concat() can only take string as argument, encountered %s", traverser.get().getClass()));
        }

        final StringBuilder sb = new StringBuilder();
        // all null values are skipped during appending, as StringBuilder will otherwise append "null" as a string
        if (null != traverser.get()) {
            // we know there is one non-null part in the string we concat
            this.isNullTraverser = false;
            sb.append(traverser.get());
        }

        if (!this.isNullTraversal) {
            sb.append(this.traversalResult);
        }
        if (null != this.concatTraversal) {
            // For the child traversal, we process and concatenate all traverser results onto the incoming traverser
            final Iterator<String> stringResults = TraversalUtil.applyAll(traverser, this.concatTraversal);
            while (stringResults.hasNext()) {
                sb.append(stringResults.next());
            }
        }

        if (!this.isNullString) {
            sb.append(this.stringArgsResult);
        }

        if (this.isNullTraverser && this.isNullTraversal && this.isNullString) {
            return null;
        } else {
            this.isNullTraverser = true;
            return sb.toString();
        }
    }

    private String processTraversal(final Traversal.Admin<S , String> concatTraversal) {
        final StringBuilder sb = new StringBuilder();
        if (null != concatTraversal) {
//            System.out.println(concatTraversal.hasNext());
            while (concatTraversal.hasNext()) {
                final String result = concatTraversal.next();
                if (null != result) {
                    this.isNullTraversal = false;
//                    System.out.println("CONCAT TRAVERSAL===" + result);
                    sb.append(result);
                }
            }
        }
        return this.isNullTraversal? null : sb.toString();
    }

    private String processStrings(final String[] concatStrings) {
        final StringBuilder sb = new StringBuilder();
        if (null != concatStrings && concatStrings.length != 0) {
            for (final String s : concatStrings) {
                if (null != s) {
                    // we know there is one non-null part in the string we concat
                    this.isNullString = false;
                    sb.append(s);
                }
            }
        }
        return this.isNullString? null : sb.toString();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
