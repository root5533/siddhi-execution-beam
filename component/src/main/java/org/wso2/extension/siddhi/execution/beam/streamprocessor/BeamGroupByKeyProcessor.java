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

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import java.util.*;


@Extension(
        name = "groupbykey",
        namespace = "beam",
        description = "This stream processor extension performs grouping of events by key.\n" +
                " for WindowedValue objects when executing a Beam pipeline.",
        parameters = {
                @Parameter(name = "event",
                        description = "All the events of type WindowedValue arriving in chunk to execute GroupByKey transform",
                        type = {DataType.OBJECT})
        },
        examples = @Example(
                syntax = "define stream inputStream (event object);\n" +
                        "@info(name = 'query1')\n" +
                        "from inputStream#beam:groupbykey(event)\n" +
                        "select event\n" +
                        "insert into outputStream;",
                description = "This query performs Beam GroupByKey transformation provided WindowedValue<KV> as event")
)

public class BeamGroupByKeyProcessor<K, V> extends StreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BeamGroupByKeyProcessor.class);
//    private HashMap<K, ArrayList<V>> groupByKey = new HashMap();

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        synchronized (this) {
            ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);
            HashMap<K, ArrayList<V>> groupByKey = new HashMap();
            try {
                while (streamEventChunk.hasNext()) {
                    StreamEvent event = streamEventChunk.next();
                    for (int i = 0; i < event.getOutputData().length; i++) {
                        if (event.getOutputData()[i] instanceof WindowedValue) {
                            KV element = (KV) ((WindowedValue) event.getOutputData()[i]).getValue();
                            if (groupByKey.containsKey(element.getKey())) {
                                ArrayList<V> items = groupByKey.get(element.getKey());
                                items.add((V) element.getValue());
                                groupByKey.put((K) element.getKey(), items);
                            } else {
                                ArrayList<V> item = new ArrayList<>();
                                item.add((V) element.getValue());
                                groupByKey.put((K) element.getKey(), item);
                            }
                        }
                    }
                }
                for (Iterator iter = groupByKey.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry map = (Map.Entry) iter.next();
                    K key = (K) map.getKey();
                    ArrayList<V> value = (ArrayList<V>) map.getValue();
                    KV kv = KV.of(key, value);
                    StreamEvent streamEvent = new StreamEvent(0, 0, 1);
                    streamEvent.setOutputData(WindowedValue.valueInGlobalWindow(kv), 0);
                    complexEventChunk.add(streamEvent);
                }
                nextProcessor.process(complexEventChunk);
//            groupByKey.clear();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

        ArrayList<Attribute> attributes = new ArrayList<Attribute>();

        if (attributeExpressionLength != 1) {
            throw new SiddhiAppCreationException("Only 1 parameter can be specified for BeamGroupByKeyProcessor");
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
