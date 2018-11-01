package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.Iterator;

public class WriteEvaluator<InputT> {

    AppliedPTransform sink;
    CommittedBundle<InputT> bundle;
    ExecutionContext context;

    public WriteEvaluator(AppliedPTransform transform, CommittedBundle bundle, ExecutionContext context) {
        this.sink = transform;
        this.bundle = bundle;
        this.context = context;
    }

    public void execute() throws Exception {
//        DoFnOperator operator = new DoFnOperator(this.sink, this.context);
//        operator.createRunner(this.bundle);
//        operator.start();
//        for (Iterator iter = this.bundle.getValues().iterator(); iter.hasNext(); ) {
//            operator.processElement( (WindowedValue<InputT>) iter.next() );
//        }
//        operator.finish();
    }

    public void finish(AppliedPTransform finalize) {

    }

}
