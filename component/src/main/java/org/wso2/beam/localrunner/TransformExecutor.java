package org.wso2.beam.localrunner;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Decides which evaluator to create depending on transform
 */

public class TransformExecutor {

    private AppliedPTransform currentTransform;
    private Bundle bundle;

    public TransformExecutor(AppliedPTransform transform, Bundle bundle) {
        this.currentTransform = transform;
        this.bundle = bundle;
    }

    public boolean run() throws Exception {
        try {
            if (this.currentTransform.getTransform() instanceof Read.Bounded) {
//                ReadEvaluator evaluator = new ReadEvaluator(this.currentTransform);
//                SourceWrapper reader = evaluator.createSourceWrapper();
//                this.bundle.setSourceReader(reader, this.currentTransform.getOutputs());
            } else if (this.currentTransform.getTransform() instanceof ParDo.MultiOutput) {
                //Implement simplerunnerdofn for all pardo
//                PardoEvaluator evaluator = new PardoEvaluator(this.currentTransform, this.bundle);
//                evaluator.execute();
            } else if (this.currentTransform.getTransform() instanceof WriteFiles) {
                //implement right file
            } else {
                System.err.println("Not a valid transform that can be executed in Siddhi");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

}
