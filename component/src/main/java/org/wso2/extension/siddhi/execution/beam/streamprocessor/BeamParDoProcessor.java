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

package org.wso2.extension.siddhi.execution.beam.streamprocessor;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.beam.runner.siddhi.ExecutionContext;
import org.wso2.beam.runner.siddhi.SiddhiDoFnOperator;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
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

/**
 * Following extension executes Beam ParDo transform in Siddhi context.
 */
@Extension(
        name = "pardo",
        namespace = "beam",
        description = "This stream processor extension performs ParDo transformation.\n" +
                " for WindowedValue objects when executing a Beam pipeline.",
        parameters = {
                @Parameter(name = "event",
                        description = "All the events of type WindowedValue arriving" +
                                " in chunk to execute ParDo transform",
                        type = {DataType.OBJECT})
        },
        examples = @Example(
                syntax = "define stream inputStream (event object);\n" +
                        "@info(name = 'query1')\n" +
                        "from inputStream#beam:pardo(event)\n" +
                        "select event\n" +
                        "insert into outputStream;",
                description = "This query performs Beam ParDo transformation to all events arriving to inputStream")
)

public class BeamParDoProcessor extends StreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(BeamParDoProcessor.class);
    private SiddhiDoFnOperator operator;
    private ExpressionExecutor eventExecutor;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        ComplexEventChunk<StreamEvent> complexEventChunk;
        synchronized (this) {
            operator.start();
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                WindowedValue value = (WindowedValue) this.eventExecutor.execute(event);
                this.operator.processElement(value);
            }
            this.operator.finish();
            complexEventChunk = this.operator.getOutputChunk();
        }
        nextProcessor.process(complexEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

        ArrayList<Attribute> attributes = new ArrayList<Attribute>();

        if (attributeExpressionLength != 2) {
            throw new SiddhiAppCreationException("Only 2 parameters can be specified for BeamExecutionProcessor");
        }

        if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.OBJECT) {
            this.eventExecutor = attributeExpressionExecutors[0];
        } else {
            throw new SiddhiAppCreationException("First parameter must be of type Object");
        }

        //TODO check instance type(variable or constant)
        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {
            /*
             * Get beam transform here and create DoFnOperator
             */
            try {
                String beamTransform = ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                        .getValue().toString();
                ExecutionContext context = ExecutionContext.getInstance();
                AppliedPTransform transform = context.getTransfromFromName(beamTransform);
                PCollection collection = context.getCollectionFromName(beamTransform);
                this.operator = new SiddhiDoFnOperator(transform, collection);
                operator.createRunner();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        } else {
            throw new SiddhiAppCreationException("Second parameter must be of type String");
        }

        return attributes;
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once and this can be used to release.
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state on a different point of time.
     *
     * @return stateful objects of the processing element as an map
     */
    @Override
    public Map<String, Object> currentState() {

        return null;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the processing element as a map.
     *              This is the same map that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> state) {

    }
}
