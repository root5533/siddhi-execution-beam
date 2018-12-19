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

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ExecutionContext {

    private static final ExecutionContext context = new ExecutionContext();
    private DirectGraph graph;
    private Map<PCollection, CommittedBundle> bundles = new HashMap<>();
    private Map<PCollection, CommittedBundle> rootBundles = new HashMap<>();
    private HashMap<String, AppliedPTransform> transformsMap = new HashMap<>();
    private HashMap<String, PCollection> collectionsMap = new HashMap<>();

    private ExecutionContext() {}

    void setGraph(DirectGraph graph) {
        this.graph = graph;
    }

    public static ExecutionContext getInstance() {
        return context;
    }

    void addRootBundle(CommittedBundle bundle) {
        this.bundles.put(bundle.getPCollection(), bundle);
        this.rootBundles.put(bundle.getPCollection(), bundle);
    }

    Collection<CommittedBundle> getRootBundles() {
        return rootBundles.values();
    }

    DirectGraph getGraph() {
        return this.graph;
    }

    void setTransformsMap(HashMap<String, AppliedPTransform> map) {
        this.transformsMap = map;
    }

    void setCollectionsMap(HashMap<String, PCollection> map) {
        this.collectionsMap = map;
    }

    public AppliedPTransform getTransfromFromName(String key) {
        return this.transformsMap.get(key);
    }

    public PCollection getCollectionFromName(String key) {
        return this.collectionsMap.get(key);
    }

}
