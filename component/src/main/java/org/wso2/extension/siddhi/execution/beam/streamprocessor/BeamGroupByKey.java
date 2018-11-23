package org.wso2.extension.siddhi.execution.beam.streamprocessor;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.beam.runner.siddhi.ExecutionContext;
import org.wso2.beam.runner.siddhi.SiddhiDoFnOperator;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BeamGroupByKey extends StreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BeamGroupByKey.class);
    private String beamTransform;
    private SiddhiDoFnOperator operator;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        LOG.info("Processing element in >>>>> " + this.beamTransform);
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);
        try {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                for (int i=0; i<event.getOutputData().length; i++) {
                    if (event.getOutputData()[i] instanceof WindowedValue) {
                        this.operator.processElement((WindowedValue) event.getOutputData()[i], complexEventChunk);
                    }
                }
                this.operator.finish();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            nextProcessor.process(complexEventChunk);
        }

    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

        ArrayList<Attribute> attributes = new ArrayList<Attribute>();

        if (attributeExpressionLength != 2) {
            throw new SiddhiAppCreationException("Only 2 parameters can be specified for BeamExecutionProcessor");
        }

        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {
            /**
             * Get beam transform here and create DoFnOperator
             */
            try {
                this.beamTransform = ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue().toString();
                ExecutionContext context = ExecutionContext.getContext();
                AppliedPTransform transform = context.getTransfromFromName(this.beamTransform);
                PCollection collection = context.getCollectionFromName(this.beamTransform);
                SiddhiDoFnOperator operator = new SiddhiDoFnOperator(transform, collection);
                operator.createRunner();
                operator.start();
                this.operator = operator;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new SiddhiAppCreationException("Second parameter must be of type String");
        }

        return attributes;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }
}
