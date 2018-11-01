package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;

public class PardoEvaluator {

    AppliedPTransform pardo;
    CustomEvent event;

    public PardoEvaluator(AppliedPTransform transform, CustomEvent event) {
        this.pardo = transform;
        this.event = event;
    }

    public void execute(ComplexEventChunk<StreamEvent> complexEventChunk) throws Exception {
        DoFnOperator operator = new DoFnOperator(this.pardo, complexEventChunk);
        operator.createRunner(this.event);
        operator.start();
        operator.processElement(this.event.getElement());
        operator.finish();
    }

}
