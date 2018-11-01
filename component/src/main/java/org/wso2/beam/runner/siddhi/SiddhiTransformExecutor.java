package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;

public class SiddhiTransformExecutor {

    public static void process(CustomEvent event, ComplexEventChunk<StreamEvent> complexEventChunk) throws Exception {
        ExecutionContext context = ExecutionContext.getContext();
        AppliedPTransform transform = context.getStartTransform();
        if (transform.getTransform() instanceof ParDo.MultiOutput) {
            PardoEvaluator evaluator = new PardoEvaluator(transform, event);
            evaluator.execute(complexEventChunk);
        }
    }

}
