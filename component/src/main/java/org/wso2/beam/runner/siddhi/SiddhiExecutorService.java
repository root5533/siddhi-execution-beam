package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
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
            executionRuntime.createSiddhiQuery();
            executionRuntime.createSiddhiRuntime();
            executionRuntime.setBundle(new CommittedBundle(null));
            context.setTransformsMap(executionRuntime.getTransformsMap());
            context.setCollectionsMap(executionRuntime.getCollectionsMap());

            /*
            Emit elements to SiddhiApp
             */
            for (Iterator roots = context.getRootBundles().iterator(); roots.hasNext(); ) {
                CommittedBundle<SourceWrapper> rootBundle = (CommittedBundle<SourceWrapper>) roots.next();
                if (!rootBundle.getPCollection().getName().equals("Readfile/Read.out")) {
                    continue;
                }
                SourceWrapper source = rootBundle.getSourceWrapper();
                source.open();
                List<AppliedPTransform<?, ?, ?>> transforms = graph.getPerElementConsumers(rootBundle.getPCollection());
                for ( Iterator iter = transforms.iterator(); iter.hasNext(); ) {
                    AppliedPTransform transform = (AppliedPTransform) iter.next();
                    String inputStream = transform.getFullName().replace('/', '_').replace('(', '_').replace(")", "") + "Stream";
                    source.run(executionRuntime.getSiddhiRuntime().getInputHandler(inputStream));
                }

                /*
                Finalize output WriteFile
                 */
                CommittedBundle bundle = executionRuntime.getBundle();
                bundle.setPCollection(context.getFinalCollection());
                if (bundle.getValues().peek() != null) {
                    for (Iterator iter = graph.getAllPerElementConsumers().asMap().values().iterator(); iter.hasNext(); ) {
                        List transformList = (List) iter.next();
                        AppliedPTransform transform = (AppliedPTransform) transformList.get(0);
                        if (transform.getFullName().equals("Writefile/WriteFiles/WriteUnshardedBundlesToTempFiles/WriteUnshardedBundles")) {
                            WriteEvaluator eval = new WriteEvaluator(transform, bundle, context);
                            eval.execute();
                            for (Iterator iterator = graph.getAllPerElementConsumers().asMap().values().iterator(); iterator.hasNext(); ) {
                                transformList = (List) iterator.next();
                                transform = (AppliedPTransform) transformList.get(0);
                                if (transform.getFullName().equals("Writefile/WriteFiles/FinalizeTempFileBundles/Finalize/ParMultiDo(Finalize)")) {
                                    bundle = context.getFinalBundle();
                                    eval = new WriteEvaluator(transform, bundle, context);
                                    eval.execute();
                                    break;
                                }
                            }
                            break;
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
