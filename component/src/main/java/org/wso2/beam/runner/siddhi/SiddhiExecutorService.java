package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
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
            Create SiddhiAppRuntime
             */
            SiddhiApp executionRuntime = new SiddhiApp();
            /*
            Emit elements to SiddhiApp
             */
            for (Iterator roots = context.getRootBundles().iterator(); roots.hasNext(); ) {
                CommittedBundle<SourceWrapper> rootBundle = (CommittedBundle<SourceWrapper>) roots.next();
                SourceWrapper source = rootBundle.getSourceWrapper();
                PCollection pCol = rootBundle.getPCollection();
                List<AppliedPTransform<?, ?, ?>> transforms = graph.getPerElementConsumers(rootBundle.getPCollection());
                AppliedPTransform<?, ?, ?> currentTransform = transforms.get(0);
                context.setStartTransform(currentTransform);

                PCollection key = null;
                for (Iterator iter = currentTransform.getOutputs().values().iterator(); iter.hasNext(); ) {
                    key = (PCollection) iter.next();
                }
                executionRuntime.setBundle(new CommittedBundle(key));
                source.open();
                source.run(executionRuntime.getSiddhiRuntime().getInputHandler("inputStream"), pCol);

                for (Iterator iter = context.getStartTransform().getOutputs().values().iterator(); iter.hasNext(); ) {
                    key = (PCollection) iter.next();
                }

                CommittedBundle bundle = executionRuntime.getBundle();
                if (bundle.getValues().peek() != null) {
                    AppliedPTransform<?, ?, ?> writeTransform = null;
                    if (key != null) {
                        transforms = graph.getPerElementConsumers(bundle.getPCollection());
                        writeTransform = transforms.get(0);
                        PCollection col = (PCollection) writeTransform.getOutputs().values().toArray()[0];
                        transforms = graph.getPerElementConsumers(col);
                        writeTransform = transforms.get(0);
                        if (writeTransform != null) {
                            WriteEvaluator eval = new WriteEvaluator(writeTransform, bundle, context);
                            eval.execute();
                        }
                    }

                    for (Iterator iter = graph.getAllPerElementConsumers().asMap().values().iterator(); iter.hasNext(); ) {
                        List transformList = (List) iter.next();
                        AppliedPTransform transform = (AppliedPTransform) transformList.get(0);
                        if (transform.getFullName().equals("Writefile/WriteFiles/FinalizeTempFileBundles/Finalize/ParMultiDo(Finalize)")) {
                            bundle = context.getFinalBundle();
                            WriteEvaluator eval = new WriteEvaluator(transform, bundle, context);
                            eval.execute();
                        }
                    }
                }
            }

            LOG.info("Siddhi Runner Complete");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
