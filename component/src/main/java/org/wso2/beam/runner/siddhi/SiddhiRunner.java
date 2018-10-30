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
        LOG.info("Executing local runner");
        GraphVisitor graphVisitor = new GraphVisitor();
        pipeline.traverseTopologically(graphVisitor);
        DirectGraph graph = graphVisitor.getGraph();
        SiddhiExecutorService executor = SiddhiExecutorService.create(targetParallelism);
        executor.start(graph);
        return null;
    }

}
