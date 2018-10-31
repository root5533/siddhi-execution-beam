package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowedValue;

public class SiddhiTransformExecutor {

    public static CustomEvent process(CustomEvent event) {
        ExecutionContext context = ExecutionContext.getContext();
        AppliedPTransform transform = context.getStartTransform();
        if (transform.getTransform() instanceof ParDo.MultiOutput) {

        }
        return null;
    }

}
