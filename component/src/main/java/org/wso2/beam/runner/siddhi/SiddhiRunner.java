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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SiddhiRunner extends PipelineRunner<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiRunner.class);
    private final SiddhiPipelineOptions options;

    public static SiddhiRunner fromOptions(PipelineOptions options) {
        SiddhiPipelineOptions localOptions = PipelineOptionsValidator.validate(SiddhiPipelineOptions.class, options);
        return new SiddhiRunner(localOptions);
    }

    private SiddhiRunner(SiddhiPipelineOptions options) {
        this.options = options;
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
        int targetParallelism = 1;
        GraphVisitor graphVisitor = new GraphVisitor();
        pipeline.traverseTopologically(graphVisitor);
        DirectGraph graph = graphVisitor.getGraph();
        SiddhiExecutorService executor = SiddhiExecutorService.create(targetParallelism);
        executor.start(graph);
        return null;
    }

}
