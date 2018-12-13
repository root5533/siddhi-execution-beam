/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.beam.runner.siddhi;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GraphVisitor extends Pipeline.PipelineVisitor.Defaults {

    private static final Logger LOG = LoggerFactory.getLogger(GraphVisitor.class);
//    private Map<AppliedPTransform<?, ?, ?>, String> stepNames = new HashMap();
    private Set<AppliedPTransform<?, ?, ?>> rootTransforms = new HashSet();
    private ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers = ArrayListMultimap.create();
    private ListMultimap<PValue, AppliedPTransform<?, ?, ?>> allConsumers = ArrayListMultimap.create();
//    private Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers = new HashMap();
    private int numTransforms = 0;
    private int depth;

    public CompositeBehavior enterCompositeTransform(Node node) {
        if (node.getTransform() instanceof TextIO.Write) {
            AppliedPTransform<?, ?, ?> appliedPTransform = this.getAppliedTransform(node);
//            this.stepNames.put(appliedPTransform, this.genStepName());
            Collection<PValue> mainInputs = TransformInputs.nonAdditionalInputs(node.toAppliedPTransform(this.getPipeline()));
            Iterator iter = mainInputs.iterator();
            PValue value;
            while(iter.hasNext()) {
                value = (PValue) iter.next();
                this.perElementConsumers.put(value, appliedPTransform);
            }
            iter = node.getInputs().values().iterator();
            while(iter.hasNext()) {
                value = (PValue) iter.next();
                this.allConsumers.put(value, appliedPTransform);
            }
            return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
        }
//        LOG.info("{} enterCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
        ++this.depth;
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    public void leaveCompositeTransform(Node node) {
        --this.depth;
//        LOG.info("{} leaveCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
    }

    public void visitPrimitiveTransform(Node node) {
//        LOG.info("{} visitPrimitiveTransform- {}", genSpaces(this.depth), node.getFullName());

        AppliedPTransform<?, ?, ?> appliedPTransform = this.getAppliedTransform(node);
//        this.stepNames.put(appliedPTransform, this.genStepName());
        if (node.getInputs().isEmpty()) {
            if (appliedPTransform.getTransform() instanceof Read.Bounded) {
                this.rootTransforms.add(appliedPTransform);
            }
        } else {
            Collection<PValue> mainInputs = TransformInputs.nonAdditionalInputs(node.toAppliedPTransform(this.getPipeline()));
            if (!mainInputs.containsAll(node.getInputs().values())) {
//                LOG.info("Inputs reduced to {} from {} by removing additional inputs", mainInputs, node.getInputs().values());
            }
            Iterator iter = mainInputs.iterator();
            PValue value;
            while(iter.hasNext()) {
                value = (PValue) iter.next();
                this.perElementConsumers.put(value, appliedPTransform);
            }
            iter = node.getInputs().values().iterator();
            while(iter.hasNext()) {
                value = (PValue) iter.next();
                this.allConsumers.put(value, appliedPTransform);
            }
        }
    }

//    public void visitValue(PValue value, Node node) {
////        LOG.info("{} visitValue- {}", genSpaces(this.depth), node.getFullName());
//        AppliedPTransform<?, ?, ?> appliedTransform = this.getAppliedTransform(node);
//        if (value instanceof PCollection && !this.producers.containsKey(value)) {
//            this.producers.put((PCollection)value, appliedTransform);
//        }
//    }

    private AppliedPTransform<?, ?, ?> getAppliedTransform(Node node) {
//        LOG.info("{} getAppliedTransform- {}", genSpaces(this.depth), node.getFullName());
        return node.toAppliedPTransform(this.getPipeline());
    }

    public DirectGraph getGraph() {
//        LOG.info("{} getGraph- {}", genSpaces(this.depth));
        return DirectGraph.create(perElementConsumers, rootTransforms);
    }

//    protected static String genSpaces(int n) {
//        StringBuilder builder = new StringBuilder();
//
//        for(int i = 0; i < n; ++i) {
//            builder.append("|   ");
//        }
//
//        return builder.toString();
//    }

//    private String genStepName() {
//        return String.format("s%s", this.numTransforms++);
//    }


}
