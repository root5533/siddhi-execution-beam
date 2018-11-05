package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;

import java.util.Iterator;
import java.util.List;

public class SiddhiTransformExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiTransformExecutor.class);
    private static ComplexEventChunk complexEventChunk;
    private static AppliedPTransform currentTransform;
    private static DirectGraph graph;
//    private static PCollection eventBundle;

    public static void process(CustomEvent event, ComplexEventChunk<StreamEvent> complexEventChunk) throws Exception {
        SiddhiTransformExecutor.complexEventChunk = complexEventChunk;
        ExecutionContext context = ExecutionContext.getContext();
        SiddhiTransformExecutor.graph = context.getGraph();
        SiddhiTransformExecutor.currentTransform = context.getStartTransform();
        if (currentTransform != null) {
            if (currentTransform.getTransform() instanceof ParDo.MultiOutput) {
                PardoEvaluator evaluator = new PardoEvaluator(currentTransform, event, event.getPCollection());
                evaluator.execute();
            } else {
                throw new SiddhiAppCreationException(currentTransform.getTransform().toString() + " is currently not supported by siddhi");
            }
        } else {
            throw new SiddhiAppCreationException("No transforms are defined");
        }
    }

    /*
    Only works for one output collection in transform
     */
    public static void process(WindowedValue event, PCollection collection) throws Exception {
        List<AppliedPTransform<?, ?, ?>> transforms = graph.getPerElementConsumers(collection);
        AppliedPTransform transform = transforms.get(0);
        if (transform != null) {
            if (transform.getTransform() instanceof ParDo.MultiOutput) {
                for ( Iterator iter = transform.getOutputs().values().iterator(); iter.hasNext(); ) {
                    PCollection bundle = (PCollection) iter.next();
                    CustomEvent newEvent = CustomEvent.create(event, bundle);
                    PardoEvaluator evaluator = new PardoEvaluator(transform, newEvent, bundle);
                    evaluator.execute();
                }
            } else {
                addEventToChunk(event);
                LOG.info("Siddhi does not support " + transform.getTransform().toString() + " at the moment");
            }
        } else {
            addEventToChunk(event);
        }
    }

    private static void addEventToChunk(WindowedValue event) {
        StreamEvent streamEvent = new StreamEvent(0, 0, 1);
        streamEvent.setOutputData(event, 0);
        complexEventChunk.add(streamEvent);
    }

}
