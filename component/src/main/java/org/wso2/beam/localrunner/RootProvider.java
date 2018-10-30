package org.wso2.beam.localrunner;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.commons.collections4.map.HashedMap;
//import org.apache.commons.collections.map.HashedMap;

import java.util.Map;

public class RootProvider {

    PipelineOptions options;

    public RootProvider(PipelineOptions options) {
        this.options = options;
    }

    public Map<WindowedValue, AppliedPTransform<?, ?, ?>> getInitialInputs(AppliedPTransform<?, ?, ?> transform, int targetParallelism) throws Exception {

        //Decide whether bounded or bounded
        Map<WindowedValue, AppliedPTransform<?, ?, ?>> inputBundle = new HashedMap<>();
        if (transform.getTransform() instanceof Read.Bounded) {
            BoundedReadEvaluator eval = BoundedReadEvaluator.create(this.options);
            inputBundle = eval.getInitialInputs(transform, targetParallelism);
        }
        if (inputBundle.size() == 0) {
            return null;
        } else {
            return inputBundle;
        }

    }

}
