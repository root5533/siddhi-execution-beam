package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;

public class PardoEvaluator {

    AppliedPTransform pardo;
    CustomEvent event;
    PCollection collection;

    public PardoEvaluator(AppliedPTransform transform, CustomEvent event, PCollection collection) {
        this.pardo = transform;
        this.event = event;
        this.collection = collection;
    }

    public void execute() throws Exception {
        SiddhiDoFnOperator operator = new SiddhiDoFnOperator(this.pardo, this.collection);
        operator.createRunner(this.event);
        operator.start();
        operator.processElement(this.event.getElement());
        operator.finish();
    }

}
