package org.wso2.beam.localrunner;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.util.Iterator;

public class ReadEvaluator<T> {

    AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform;
    ExecutionContext context;

    public ReadEvaluator(AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform, ExecutionContext context) {
        this.transform = transform;
        this.context = context;
    }

    public void execute() throws Exception {
        Read.Bounded boundedInput = (Read.Bounded) this.transform.getTransform();
        BoundedSource<T> source = boundedInput.getSource();
        SourceWrapper sourceWrapper = new SourceWrapper(source, 1, transform.getPipeline().getOptions());
        for (Iterator iter = this.transform.getOutputs().values().iterator(); iter.hasNext();) {
            CommittedBundle<SourceWrapper> bundle = new CommittedBundle<>((PCollection) iter.next());
            bundle.addItem(sourceWrapper);
            this.context.addRootBundle(bundle);
        }
    }

}
