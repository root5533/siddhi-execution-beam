package org.wso2.beam.runner.siddhi;

import org.apache.beam.runners.core.*;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SiddhiDoFnOperator<InputT, OutputT> {

    private final AppliedPTransform<?, ?, ?> transform;
    private DoFnRunner<InputT, OutputT> delegate;
    private PCollection collection;
    private ComplexEventChunk complexEventChunk;
    private PipelineOptions options;
    private SideInputReader sideInputReader;
    private OutputManager outputManager;
    private TupleTag<OutputT> mainOutputTag;
    private List<TupleTag<?>> additionalOutputTags;
    private StepContext stepContext;
    private Coder<InputT> inputCoder;
    private Map<TupleTag<?>, Coder<?>> outputCoders;
    private WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy;
    private DoFn<InputT, OutputT> fn;

    public SiddhiDoFnOperator(AppliedPTransform transform, PCollection collection) {
        this.transform = transform;
        this.collection = collection;
    }

    public void createRunner() throws Exception {
        this.options = this.transform.getPipeline().getOptions();
        this.sideInputReader = SiddhiDoFnOperator.LocalSideInputReader.create(ParDoTranslation.getSideInputs(this.transform));
        this.outputManager = new SiddhiDoFnOperator.BundleOutputManager();
        this.mainOutputTag = (TupleTag<OutputT>) ParDoTranslation.getMainOutputTag(this.transform);
        this.additionalOutputTags = ParDoTranslation.getAdditionalOutputTags(this.transform).getAll();
        this.stepContext = SiddhiDoFnOperator.LocalStepContext.create();
        this.inputCoder = this.collection.getCoder();
        this.outputCoders = (Map)this.transform.getOutputs().entrySet().stream().collect(Collectors.toMap((e) -> {
            return (TupleTag)e.getKey();
        }, (e) -> {
            return ((PCollection)e.getValue()).getCoder();
        }));
        this.windowingStrategy = this.collection.getWindowingStrategy();
        DoFn<InputT, OutputT> fn = this.getDoFn(this.transform.getTransform());
        this.delegate = new SimpleDoFnRunner(options, fn, sideInputReader, outputManager, mainOutputTag, additionalOutputTags, stepContext, inputCoder, outputCoders, windowingStrategy);
    }

    public void start() {
        this.delegate.startBundle();
    }

    public void finish() {
        this.delegate.finishBundle();
    }

    public void processElement(WindowedValue<InputT> element, ComplexEventChunk complexEventChunk) {
        this.complexEventChunk = complexEventChunk;
        try {
            this.delegate.processElement(element);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DoFn getDoFn(PTransform transform) {
        if (transform instanceof ParDo.MultiOutput) {
            return ((ParDo.MultiOutput) this.transform.getTransform()).getFn();
        }
        if (transform instanceof GroupByKey) {
            return GroupAlsoByWindowViaWindowSetNewDoFn.create(this.windowingStrategy, stateInternalsFactory, timerInternalsFactory, this.sideInputReader, this.systemReduceFn, this.outputManager, this.mainOutputTag);
        }
        return null;
    }

    private class BundleOutputManager implements OutputManager {

        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            try {
                StreamEvent streamEvent = new StreamEvent(0, 0, 1);
                streamEvent.setOutputData(output, 0);
                SiddhiDoFnOperator.this.complexEventChunk.add(streamEvent);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class LocalStepContext implements StepContext {

        public static SiddhiDoFnOperator.LocalStepContext create() {
            return new SiddhiDoFnOperator.LocalStepContext();
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
