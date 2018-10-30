package org.wso2.beam.localrunner;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Hard coded execution of {@link org.wso2.beam.localrunner.TransformExecutor}
 */

public class SimpleLocalRunnerService {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleLocalRunnerService.class);
    private final int targetParallelism;

    public static SimpleLocalRunnerService create(int targetParallelism) {
        return new SimpleLocalRunnerService(targetParallelism);
    }

    private SimpleLocalRunnerService(int targetParallelism) {
        this.targetParallelism = targetParallelism;
    }

    public void start(DirectGraph graph, RootProvider rootProvider) {
        try {
            /**
             * Execute ReadFile/Read to generate split sources
             */
            ExecutionContext context = new ExecutionContext(graph);
            for (Iterator iter = graph.getRootTransforms().iterator(); iter.hasNext(); ) {
                AppliedPTransform root = (AppliedPTransform) iter.next();
                if (root.getFullName().equals("Readfile/Read")) {
                    ReadEvaluator evaluator = new ReadEvaluator(root, context);
                    evaluator.execute();
                }
            }

            /**
             * Execute ParDoTransform
             */
            CommittedBundle<SourceWrapper> rootBundle = context.getPendingRootBundle();
            List<AppliedPTransform<?, ?, ?>> transforms = graph.getPerElementConsumers(rootBundle.getPCollection());
            AppliedPTransform<?, ?, ?> pardoTransform = transforms.get(0);
            PardoEvaluator evaluator = new PardoEvaluator(pardoTransform, rootBundle, context);
            evaluator.execute();

            /**
             * Execute WriteFile
             */
            PCollection key = null;
            for (Iterator iter = pardoTransform.getOutputs().values().iterator(); iter.hasNext(); ) {
                key = (PCollection) iter.next();
            }


            AppliedPTransform<?, ?, ?> writeTransform = null;
            if (key != null) {
                CommittedBundle<WindowedValue> bundle = context.getBundle(key);
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

            for (Iterator iter = graph.getAllPerElementConsumers().asMap().values().iterator(); iter.hasNext();) {
                List transformList = (List) iter.next();
                AppliedPTransform transform = (AppliedPTransform) transformList.get(0);
                if (transform.getFullName().equals("Writefile/WriteFiles/FinalizeTempFileBundles/Finalize/ParMultiDo(Finalize)")) {
                    CommittedBundle bundle = context.getFinalBundle();
                    WriteEvaluator eval = new WriteEvaluator(transform, bundle, context);
                    eval.execute();
                }
            }

//            for (int i = 0; i<10; i++) {
//                PCollection col = (PCollection) writeTransform.getOutputs().values().toArray()[0];
//                transforms = graph.getPerElementConsumers(col);
//                writeTransform = transforms.get(0);
//                System.out.println("writeTransform : " + writeTransform.getFullName());
//            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
