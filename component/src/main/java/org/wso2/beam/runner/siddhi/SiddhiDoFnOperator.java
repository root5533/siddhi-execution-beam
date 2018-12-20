/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.beam.runner.siddhi;

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import java.util.List;
import java.util.Map;

/**
 * Creates and manages {@link SimpleDoFnRunner} given the {@link PTransform} and {@link PCollection}
 * in order to perform {@link ParDo} transform.
 * @param <InputT>
 * @param <OutputT>
 * @param <TransformT>
 */
public class SiddhiDoFnOperator
        <InputT extends PInput, OutputT extends POutput, TransformT extends PTransform<InputT, OutputT>> {

    private final AppliedPTransform<InputT, OutputT, TransformT> transform;
    private DoFnRunner<InputT, OutputT> delegate;
    private PCollection<InputT> collection;
    private ComplexEventChunk<StreamEvent> complexEventChunk;
    protected PipelineOptions options;
    private SideInputReader sideInputReader;
    private OutputManager outputManager;
    private TupleTag mainOutputTag;
    private List<TupleTag<?>> additionalOutputTags;
    private StepContext stepContext;
    private Coder<InputT> inputCoder;
    private Map<TupleTag<?>, Coder<?>> outputCoders;
    private WindowingStrategy windowingStrategy;
    private DoFn<InputT, OutputT> fn;

    public SiddhiDoFnOperator(
            AppliedPTransform<InputT, OutputT, TransformT> transform, PCollection<InputT> collection) {
        this.transform = transform;
        this.collection = collection;
    }

    public void createRunner() throws Exception {
        this.options = this.transform.getPipeline().getOptions();
        this.sideInputReader = SiddhiDoFnOperator.LocalSideInputReader
                .create(ParDoTranslation.getSideInputs(this.transform));
        this.outputManager = new SiddhiDoFnOperator.BundleOutputManager();
        this.mainOutputTag = ParDoTranslation.getMainOutputTag(this.transform);
        this.additionalOutputTags = ParDoTranslation.getAdditionalOutputTags(this.transform).getAll();
        this.stepContext = SiddhiDoFnOperator.LocalStepContext.create();
        this.inputCoder = this.collection.getCoder();
        this.outputCoders = null;
        this.windowingStrategy = this.collection.getWindowingStrategy();
        this.fn = this.getDoFn();
        this.delegate = new SimpleDoFnRunner<InputT, OutputT>(options, fn, sideInputReader, outputManager,
                mainOutputTag, additionalOutputTags, stepContext, inputCoder, outputCoders, windowingStrategy);
    }

    public void start() {
        this.delegate.startBundle();
        this.complexEventChunk = new ComplexEventChunk<>(false);
    }

    public void finish() {
        this.delegate.finishBundle();
    }

    public void processElement(WindowedValue<InputT> element) {
        this.delegate.processElement(element);
    }

    private DoFn<InputT, OutputT> getDoFn() {
        return ((ParDo.MultiOutput<InputT, OutputT>) this.transform.getTransform()).getFn();
    }

    public ComplexEventChunk<StreamEvent> getOutputChunk() {
        return this.complexEventChunk;
    }

    /**
     *
     */
    protected class BundleOutputManager implements OutputManager {

        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            StreamEvent streamEvent = new StreamEvent(0, 0, 1);
            streamEvent.setOutputData(output, 0);
            SiddhiDoFnOperator.this.complexEventChunk.add(streamEvent);
        }
    }

    /**
     *
     */
    protected static class LocalStepContext implements StepContext {

        public static SiddhiDoFnOperator.LocalStepContext create() {
            return new SiddhiDoFnOperator.LocalStepContext();
        }

        @Override
        public StateInternals stateInternals() {
            return null;
        }

        @Override
        public TimerInternals timerInternals() {
            return null;
        }
    }

    /**
     *
     */
    protected static class LocalSideInputReader implements SideInputReader {

        public static LocalSideInputReader create(List<PCollectionView<?>> sideInputReader) {
            return new LocalSideInputReader();
        }

        @Override
        public <T> T get(PCollectionView<T> view, BoundedWindow window) {
            return null;
        }

        @Override
        public <T> boolean contains(PCollectionView<T> view) {
            return false;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }

}
