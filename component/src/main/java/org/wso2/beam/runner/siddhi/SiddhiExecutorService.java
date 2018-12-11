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
                AppliedPTransform root = (AppliedPTransform) iter.next();
                ReadEvaluator evaluator = new ReadEvaluator(root);
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

                /*
                Finalize output WriteFile
                 */
//                Thread.sleep(3000);
//                CommittedBundle bundle = executionRuntime.getBundle();
//                bundle.setPCollection(executionRuntime.getFinalCollection());
//                if (bundle.getValues().peek() != null) {
//                    for (Iterator iter = graph.getAllPerElementConsumers().asMap().values().iterator(); iter.hasNext(); ) {
//                        List transformList = (List) iter.next();
//                        AppliedPTransform transform = (AppliedPTransform) transformList.get(0);
//                        if (transform.getFullName().equals("Writefile/WriteFiles/WriteUnshardedBundlesToTempFiles/WriteUnshardedBundles")) {
//                            WriteEvaluator eval = new WriteEvaluator(transform, bundle, context);
//                            eval.execute();
//                            for (Iterator iterator = graph.getAllPerElementConsumers().asMap().values().iterator(); iterator.hasNext(); ) {
//                                transformList = (List) iterator.next();
//                                transform = (AppliedPTransform) transformList.get(0);
//                                if (transform.getFullName().equals("Writefile/WriteFiles/FinalizeTempFileBundles/Finalize/ParMultiDo(Finalize)")) {
//                                    bundle = context.getFinalBundle();
//                                    eval = new WriteEvaluator(transform, bundle, context);
//                                    eval.execute();
//                                    break;
//                                }
//                            }
//                            break;
//                        }
//                    }
//                } else {
//                    LOG.info("***No data in bundle to write!***");
//                }

//                CommittedBundle bundle = executionRuntime.getBundle();
//                bundle.setPCollection(executionRuntime.getFinalCollection());
//                if (bundle.getValues().peek() != null) {
//                    LOG.info("***Data in bundle to write!***");
//                } else {
//                    LOG.info("***No data in bundle to write!***");
//                }
            }
            LOG.info("Siddhi Runner Complete");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
