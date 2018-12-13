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
import org.apache.beam.sdk.values.*;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SiddhiDoFnOperator<InputT, OutputT> {

    protected final AppliedPTransform<?, ?, ?> transform;
    protected DoFnRunner<InputT, OutputT> delegate;
    protected PCollection collection;
    protected ComplexEventChunk complexEventChunk;
    protected PipelineOptions options;
    protected SideInputReader sideInputReader;
    protected OutputManager outputManager;
    protected TupleTag<OutputT> mainOutputTag;
    protected List<TupleTag<?>> additionalOutputTags;
    protected StepContext stepContext;
    protected Coder<InputT> inputCoder;
    protected Map<TupleTag<?>, Coder<?>> outputCoders;
    protected WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy;
    protected DoFn<InputT, OutputT> fn;

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
        this.fn = this.getDoFn();
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

    private DoFn getDoFn() {
        return ((ParDo.MultiOutput) this.transform.getTransform()).getFn();
    }

    protected class BundleOutputManager implements OutputManager {

        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            try {
                StreamEvent streamEvent = new StreamEvent(0, 0, 1);
                streamEvent.setOutputData(output, 0);
                if (SiddhiDoFnOperator.this.complexEventChunk == null) {
                    System.out.println("complex event chunk is null");
                }
                SiddhiDoFnOperator.this.complexEventChunk.add(streamEvent);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected static class LocalStepContext implements StepContext {

        public static SiddhiDoFnOperator.LocalStepContext create() {
            return new SiddhiDoFnOperator.LocalStepContext();
        }

        @Override
        public StateInternals stateInternals() {
//            System.out.println("DoFnOperator : LocalStepContext : stateInternals()");
            return null;
        }

        @Override
        public TimerInternals timerInternals() {
//            System.out.println("DoFnOperator : LocalStepContext : timerInternals()");
            return null;
        }
    }

    protected static class LocalSideInputReader implements SideInputReader {

        public static LocalSideInputReader create(List<PCollectionView<?>> sideInputReader) {
            return new LocalSideInputReader();
        }

        @Override
        public <T> T get(PCollectionView<T> view, BoundedWindow window) {
//            System.out.println("DoFnOperator : LocalSideInputReader : get()");
            return null;
        }

        @Override
        public <T> boolean contains(PCollectionView<T> view) {
//            System.out.println("DoFnOperator : LocalSideInputReader : contains()");
            return false;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }

}
