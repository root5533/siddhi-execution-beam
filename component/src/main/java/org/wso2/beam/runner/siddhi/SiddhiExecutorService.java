package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiAppRuntime;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class SiddhiExecutorService {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiExecutorService.class);
    private final int targetParallelism;

    public static SiddhiExecutorService create(int targetParallelism) {
        return new SiddhiExecutorService(targetParallelism);
    }

    private SiddhiExecutorService(int targetParallelism) {
        this.targetParallelism = targetParallelism;
    }

    public void start(DirectGraph graph) {
        LOG.info("Starting Siddhi Runner");
        try {
            /*
             * Generate sources from root transforms
             */
            ExecutionContext context = ExecutionContext.getContext();
            context.setGraph(graph);
            for (Iterator iter = graph.getRootTransforms().iterator(); iter.hasNext(); ) {
                AppliedPTransform root = (AppliedPTransform) iter.next();
                ReadEvaluator evaluator = new ReadEvaluator(root);
                evaluator.execute(this.targetParallelism);
            }

            /*
             * Create SiddhiAppRuntime
             */
            SiddhiAppRuntime executionRuntime = SiddhiApp.create();

            /*
            Emit elements to SiddhiApp
             */
            for (Iterator iter = context.getRootBundles().iterator(); iter.hasNext(); ) {
                CommittedBundle<SourceWrapper> rootBundle = (CommittedBundle<SourceWrapper>) iter.next();
                SourceWrapper source = rootBundle.getSourceWrapper();
                PCollection pCol = rootBundle.getPCollection();
                List<AppliedPTransform<?, ?, ?>> transforms = graph.getPerElementConsumers(rootBundle.getPCollection());
                AppliedPTransform<?, ?, ?> startTransform = transforms.get(0);
                context.setStartTransform(startTransform);
                source.open();
                source.run(executionRuntime.getInputHandler("inputStream"), pCol);
            }

            LOG.info("Siddhi Runner Complete");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
