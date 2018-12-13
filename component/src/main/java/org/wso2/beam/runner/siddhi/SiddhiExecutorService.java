package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                AppliedPTransform rootTransform = (AppliedPTransform) iter.next();
                ReadEvaluator evaluator = new ReadEvaluator(rootTransform);
                evaluator.execute(this.targetParallelism);
            }

            /*
            Create SiddhiAppRuntime
             */
            SiddhiApp executionRuntime = new SiddhiApp();
            executionRuntime.createSiddhiQuery();
            context.setTransformsMap(executionRuntime.getTransformsMap());
            context.setCollectionsMap(executionRuntime.getCollectionsMap());
            executionRuntime.createSiddhiRuntime();
            executionRuntime.setBundle(new CommittedBundle(null));

            /*
            Emit elements to SiddhiApp
             */
            for (Iterator roots = context.getRootBundles().iterator(); roots.hasNext(); ) {
                CommittedBundle<SourceWrapper> rootBundle = (CommittedBundle<SourceWrapper>) roots.next();
                SourceWrapper source = rootBundle.getSourceWrapper();
                source.open();
                List<AppliedPTransform<?, ?, ?>> transforms = graph.getPerElementConsumers(rootBundle.getPCollection());
                for ( Iterator iter = transforms.iterator(); iter.hasNext(); ) {
                    AppliedPTransform transform = (AppliedPTransform) iter.next();
                    String inputStream = SiddhiApp.generateTransformName(transform.getFullName()) + "Stream";
                    source.run(executionRuntime.getSiddhiRuntime().getInputHandler(inputStream));
                }
            }
            LOG.info("Executing pipeline");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
