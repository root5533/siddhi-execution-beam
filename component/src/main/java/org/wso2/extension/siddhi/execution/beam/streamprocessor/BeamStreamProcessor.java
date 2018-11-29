package org.wso2.extension.siddhi.execution.beam.streamprocessor;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.beam.runner.siddhi.ExecutionContext;
import org.wso2.beam.runner.siddhi.GroupDoFnOperator;
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

/**
 * This is a sample class-level comment, explaining what the extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter",
 *                               description= "The description of the first parameter",
 *                               type =  "Supported parameter types.
 *                                        eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         )",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                          according to the type."),
 * {@literal @}Parameter(name = "The name of the second parameter",
 *                               description= "The description of the second parameter",
 *                               type =   "Supported parameter types.
 *                                         eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                         according to the type."),
 * },
 * //If Source system configurations will need then
 * systemParameters = {
 * {@literal @}SystemParameter(name = "The name of the first  system parameter",
 *                                      description="The description of the first system parameter." ,
 *                                      defaultValue = "the default value of the system parameter.",
 *                                      possibleParameter="the possible value of the system parameter.",
 *                               ),
 * },
 * examples = {
 * {@literal @}Example(syntax = "sample query that explain how extension use in Siddhi."
 *                              description =" The description of the given example's query."
 *                      ),
 * }
 * )
 * </code></pre>
 */

public class BeamStreamProcessor  extends StreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BeamStreamProcessor.class);
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
            }
            this.operator.finish();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            nextProcessor.process(complexEventChunk);
        }

    }

    /**
     * The initialization method for {@link StreamProcessor}, which will be called before other methods and validate
     * the all configuration and getting the initial values.
     * @param attributeExpressionExecutors are the executors of each attributes in the Function
     * @param configReader        this hold the {@link StreamProcessor} extensions configuration reader.
     * @param siddhiAppContext    Siddhi app runtime context
     */
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
                SiddhiDoFnOperator operator;
                operator = new SiddhiDoFnOperator(transform, collection);
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
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state on a different point of time
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
