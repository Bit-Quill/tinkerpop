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

import java.util.Collections;
import java.util.Set;

public class ConcatStep<S> extends ScalarMapStep<S, String> implements TraversalParent {

    private String[] concatStrings;

    private String traversalResult;

    private Traversal.Admin<S, String> concatTraversal;

    // flag used to propagate the null value through if all strings to be concatenated are null
    private boolean isAllNull = true;

    public ConcatStep(final Traversal.Admin traversal, final String... concatStrings) {
        super(traversal);
        this.concatStrings = concatStrings;
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
            this.isAllNull = false;
            sb.append(traverser.get());
        }

        if (null != this.traversalResult) {
            sb.append(this.traversalResult);
        } else if (null != this.concatTraversal) {
            // process traversals
            sb.append(TraversalUtil.apply(traverser, this.concatTraversal));
        }

        if (null != this.concatStrings && this.concatStrings.length != 0) {
            for (final String s : this.concatStrings) {
                if (null != s) {
                    // we know there is one non-null part in the string we concat
                    this.isAllNull = false;
                    sb.append(s);
                }
            }
        }

        return this.isAllNull? null : sb.toString();
    }

    private String processTraversal(final Traversal.Admin<S , String> concatTraversal) {
        final StringBuilder sb = new StringBuilder();
        if (null != concatTraversal) {
            while (concatTraversal.hasNext()) {
                final String result = concatTraversal.next();
                if (null != result) {
                    this.isAllNull = false;
                    sb.append(result);
                }
            }
        }
        return this.isAllNull? null : sb.toString();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
