package org.wso2.beam.runner.siddhi;

import org.apache.beam.runners.core.*;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;


import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class GroupDoFnOperator extends SiddhiDoFnOperator {

    private SystemReduceFn systemReduceFn;
    private StateInternals stateInternals;
    private TimerInternals timerInternals;

    public GroupDoFnOperator(AppliedPTransform transform, PCollection collection) {
        super(transform, collection);
    }

    public void createRunner() throws Exception {
        this.options = this.transform.getPipeline().getOptions();
        this.sideInputReader = LocalSideInputReader.create(ParDoTranslation.getSideInputs(this.transform));
        this.outputManager = new BundleOutputManager();
        this.mainOutputTag = new TupleTag("group by key tag");
        this.additionalOutputTags = Collections.emptyList();
        this.stepContext = LocalStepContext.create();
        this.inputCoder = this.collection.getCoder();
        this.outputCoders = Collections.emptyMap();
        this.windowingStrategy = this.collection.getWindowingStrategy();
        this.systemReduceFn = SystemReduceFn.buffering(((KvCoder)this.collection.getCoder()).getValueCoder());
        this.fn = this.getDoFn();
        this.delegate = new SimpleDoFnRunner(options, fn, sideInputReader, outputManager, mainOutputTag, additionalOutputTags, stepContext, inputCoder, outputCoders, windowingStrategy);
        this.stateInternals = new SiddhiStateInternals();
        this.timerInternals = new SiddhiTimerInternals();
    }

    private DoFn getDoFn() {
        StateInternalsFactory stateInternalsFactory = (key) -> {
            return this.stateInternals;
        };
        TimerInternalsFactory timerInternalsFactory = (key) -> {
            return this.timerInternals;
        };
        return GroupAlsoByWindowViaWindowSetNewDoFn.create(this.windowingStrategy, stateInternalsFactory, timerInternalsFactory, this.sideInputReader, this.systemReduceFn, this.outputManager, this.mainOutputTag);
    }

}
