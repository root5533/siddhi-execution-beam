package org.wso2.beam.runner.siddhi;

import org.apache.beam.runners.core.*;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.StreamEvent;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DoFnOperator<InputT, OutputT> {

    private final AppliedPTransform<?, ?, ?> transform;
    private DoFnRunner<InputT, OutputT> delegate;
//    private CommittedBundle<WindowedValue<?>> outputBundle;
    private ComplexEventChunk<StreamEvent> complexEventChunk;

    public DoFnOperator(AppliedPTransform transform, ComplexEventChunk<StreamEvent> complexEventChunk) {
        this.transform = transform;
        this.complexEventChunk = complexEventChunk;
    }

    public void createRunner(CustomEvent event) throws Exception {
        PipelineOptions options = this.transform.getPipeline().getOptions();
        DoFn<InputT, OutputT> fn = ((ParDo.MultiOutput) this.transform.getTransform()).getFn();
        SideInputReader sideInputReader = DoFnOperator.LocalSideInputReader.create(ParDoTranslation.getSideInputs(this.transform));
        OutputManager outputManager = new DoFnOperator.BundleOutputManager();
        TupleTag<OutputT> mainOutputTag = (TupleTag<OutputT>) ParDoTranslation.getMainOutputTag(this.transform);
        List<TupleTag<?>> additionalOutputTags = ParDoTranslation.getAdditionalOutputTags(this.transform).getAll();
        StepContext stepContext = DoFnOperator.LocalStepContext.create();
        Coder<InputT> inputCoder = event.getPCollection().getCoder();
        Map<TupleTag<?>, Coder<?>> outputCoders = (Map)this.transform.getOutputs().entrySet().stream().collect(Collectors.toMap((e) -> {
            return (TupleTag)e.getKey();
        }, (e) -> {
            return ((PCollection)e.getValue()).getCoder();
        }));
        WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy = event.getPCollection().getWindowingStrategy();
        this.delegate = new SimpleDoFnRunner(options, fn, sideInputReader, outputManager, mainOutputTag, additionalOutputTags, stepContext, inputCoder, outputCoders, windowingStrategy);

        /**
         * Create Committed Bundle for output(expecting only 1)
         */
//        for (Iterator iter = this.transform.getOutputs().values().iterator(); iter.hasNext();) {
//            this.outputBundle = new CommittedBundle((PCollection) iter.next());
//        }

    }

    public void start() {
        this.delegate.startBundle();
    }

    public void finish() {
        this.delegate.finishBundle();
    }

    public void processElement(WindowedValue<InputT> element) {
        try {
            this.delegate.processElement(element);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class BundleOutputManager implements OutputManager {

        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            StreamEvent event = new StreamEvent(0,0,1);
            Object[] object = {output};
            event.setOutputData(object);
            DoFnOperator.this.complexEventChunk.add(event);
        }
    }

    private static class LocalStepContext implements StepContext {

        public static DoFnOperator.LocalStepContext create() {
            return new DoFnOperator.LocalStepContext();
        }

        @Override
        public StateInternals stateInternals() {
            System.out.println("DoFnOperator : LocalStepContext : stateInternals()");
            return null;
        }

        @Override
        public TimerInternals timerInternals() {
            System.out.println("DoFnOperator : LocalStepContext : timerInternals()");
            return null;
        }
    }

    private static class LocalSideInputReader implements SideInputReader {

        public static LocalSideInputReader create(List<PCollectionView<?>> sideInputReader) {
            return new LocalSideInputReader();
        }

        @Override
        public <T> T get(PCollectionView<T> view, BoundedWindow window) {
            System.out.println("DoFnOperator : LocalSideInputReader : get()");
            return null;
        }

        @Override
        public <T> boolean contains(PCollectionView<T> view) {
            System.out.println("DoFnOperator : LocalSideInputReader : contains()");
            return false;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }

}
