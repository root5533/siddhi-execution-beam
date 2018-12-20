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

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Elements that are output by a {@link PTransform} gets collected here.
 * @param <T> type of elements contained in this bundle
 */
public class CommittedBundle<T> {

    private PCollection bundle;
    private Queue<T> values = new LinkedList<>();

    public CommittedBundle(PCollection bundle) {
        this.bundle = bundle;
    }

    public void addItem(T item) {
        values.add(item);
    }

    public PCollection getPCollection() {
        return this.bundle;
    }

    public void setPCollection(PCollection bundle) {
        this.bundle = bundle;
    }

    public SourceWrapper getSourceWrapper() {
        return (SourceWrapper) this.values.poll();
    }

    public Queue getValues() {
        return this.values;
    }

}
