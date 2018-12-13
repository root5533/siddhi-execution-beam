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

import com.google.common.collect.ListMultimap;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;

import java.util.List;
import java.util.Set;

public class DirectGraph {

//    private final Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers;
    private final ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers;
    private final Set<AppliedPTransform<?, ?, ?>> rootTransforms;
//    private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;

    public static DirectGraph create(ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
                                     Set<AppliedPTransform<?, ?, ?>> rootTransforms) {
        return new DirectGraph(perElementConsumers, rootTransforms);
    }

    private DirectGraph(ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
                        Set<AppliedPTransform<?, ?, ?>> rootTransforms) {
//        this.producers = producers;
        this.perElementConsumers = perElementConsumers;
        this.rootTransforms = rootTransforms;
//        this.stepNames = stepNames;
    }

    public Set<AppliedPTransform<?, ?, ?>> getRootTransforms() {
        return this.rootTransforms;
    }

    public List<AppliedPTransform<?, ?, ?>> getPerElementConsumers(PValue consumed) {
        return this.perElementConsumers.get(consumed);
    }

//    public ListMultimap getAllPerElementConsumers() {
//        return this.perElementConsumers;
//    }

}
