package org.wso2.beam.localrunner;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ExecutorService {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorService.class);
    private final int targetParallelism;

    public static ExecutorService create(int targetParallelism) {
        return new ExecutorService(targetParallelism);
    }

    private ExecutorService(int targetParallelism) {
        this.targetParallelism = targetParallelism;
    }

    public void start(DirectGraph graph, RootProvider rootProvider) {

        int numOfSplits = Math.max(3, this.targetParallelism);
        Bundle bundle = Bundle.create();
        AppliedPTransform root;
        try {
            for (Iterator iter = graph.getRootTransforms().iterator(); iter.hasNext(); ) {
                root = (AppliedPTransform) iter.next();
                TransformExecutor executor = new TransformExecutor(root, bundle);
                executor.run();
            }
            HashMap<PCollection, SourceWrapper> readers = bundle.getReaders();
            for ( Iterator iter = readers.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry entry = (Map.Entry) iter.next();
                PCollection key = (PCollection) entry.getKey();
                Queue<PCollection> pending = new LinkedList<>();
                AppliedPTransform<?, ?, ?> nextTransform = graph.getPerElementConsumers(key).get(0);
                while( nextTransform != null ) {
                    AppliedPTransform<?, ?, ?> currentTransform = nextTransform;
                    System.out.println("Do transformation: " + currentTransform.getFullName());
                    TransformExecutor executor = new TransformExecutor(currentTransform, bundle);
                    boolean isExecuted = executor.run();
                    if (!isExecuted) {
                        break;
                    }
                    if ( currentTransform.getOutputs().size() != 0 ) {
                        for (Iterator iter2 = currentTransform.getOutputs().values().iterator(); iter2.hasNext(); ) {
                            pending.add((PCollection) iter2.next());
                        }
                    }
                    if (pending.isEmpty()) {
                        nextTransform = null;
                    } else {
                        List<AppliedPTransform<?, ?, ?>> transform = graph.getPerElementConsumers(pending.poll());
                        if (transform.size() != 0) {
                            nextTransform = transform.get(0);
                        } else {
                            nextTransform = null;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

//        AppliedPTransform transform;
//        for (Iterator iter = graph.getParentTransforms().iterator(); iter.hasNext();) {
//            transform = (AppliedPTransform) iter.next();
//            try {
//                TransformExecutor executor = new TransformExecutor(transform, bundle);
//                executor.run();
//            } catch (Exception e) {
//                e.printStackTrace();
//            } finally {
//                System.out.println("Execution Complete");
//            }
//        }

    }

}
