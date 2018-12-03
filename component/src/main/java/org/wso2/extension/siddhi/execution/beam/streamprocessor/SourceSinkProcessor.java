package org.wso2.extension.siddhi.execution.beam.streamprocessor;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import java.util.*;

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

public class SourceSinkProcessor<V> extends StreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SourceSinkProcessor.class);
    private String type;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        if (this.type.equals("sink")) {
            LOG.info("Transforming object to process in sink stream");
            ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);
            try {
                while(streamEventChunk.hasNext()) {
                    StreamEvent event = streamEventChunk.next();
                    for (int i=0; i<event.getOutputData().length; i++) {
                        WindowedValue element = (WindowedValue) event.getOutputData()[i];
                        V newValue = (V) element.getValue();
                        StreamEvent streamEvent = new StreamEvent(0,0,1);
                        streamEvent.setOutputData(newValue, 0);
                        complexEventChunk.add(streamEvent);
                    }
                }
                nextProcessor.process(complexEventChunk);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

//        LOG.info("Grouping elements >>>><<<<");
//        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);
//        try {
//            while (streamEventChunk.hasNext()) {
//                StreamEvent event = streamEventChunk.next();
//                for (int i=0; i<event.getOutputData().length; i++) {
//                    if (event.getOutputData()[i] instanceof WindowedValue) {
//                        KV element = (KV) ((WindowedValue) event.getOutputData()[i]).getValue();
//                        if (this.groupByKey.containsKey(element.getKey())) {
//                            ArrayList<V> items = this.groupByKey.get(element.getKey());
//                            items.add((V) element.getValue());
//                            this.groupByKey.put((K) element.getKey(), items);
//                        } else {
//                            ArrayList<V> item = new ArrayList<>();
//                            item.add((V) element.getValue());
//                            this.groupByKey.put((K) element.getKey(), item);
//                        }
//                    }
//                }
//            }
//            for (Iterator iter = this.groupByKey.entrySet().iterator(); iter.hasNext();) {
//                 Map.Entry map = (Map.Entry) iter.next();
//                 K key = (K) map.getKey();
//                 ArrayList<V> value = (ArrayList<V>) map.getValue();
//                 KV kv = KV.of(key, value);
//                 StreamEvent streamEvent = new StreamEvent(0,0,1);
//                 streamEvent.setOutputData(WindowedValue.valueInGlobalWindow(kv), 0);
//                 complexEventChunk.add(streamEvent);
//            }
//            nextProcessor.process(complexEventChunk);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }


    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

        ArrayList<Attribute> attributes = new ArrayList<Attribute>();

        if (attributeExpressionLength != 2) {
            throw new SiddhiAppCreationException("Only 2 parameters can be specified for SourceSinkProcessor");
        }

        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {

            try {
                if (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue().equals("sink")) {
                    this.type = "sink";
                    LOG.info("SourceSinkProcessor initialized for sink transform");
                } else {
                    this.type = "source";
                    LOG.info("SourceSinkProcessor initialized for source transform");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new SiddhiAppCreationException("Second parameter must be of type String to identify source or sink");
        }

        return attributes;
    }


    @Override
    public void start() { }

    @Override
    public void stop() { }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) { }
}
