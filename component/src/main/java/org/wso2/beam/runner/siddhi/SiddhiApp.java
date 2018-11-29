package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.execution.beam.streamprocessor.BeamStreamProcessor;
import org.wso2.extension.siddhi.execution.beam.streamprocessor.GroupByKeyProcessor;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.*;

public class SiddhiApp {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiApp.class);
    private SiddhiAppRuntime runtime;
    private CommittedBundle bundle;
    private Queue<String> streamDefinitions = new LinkedList<>();
    private Queue<String> queryDefinitions = new LinkedList<>();
    private HashMap<String, AppliedPTransform> transformsMap = new HashMap<>();
    private HashMap<String, PCollection> collectionsMap = new HashMap<>();
    private DirectGraph graph;
    private PCollection finalCollection;

    public SiddhiApp() { }

    public void createSiddhiQuery() {
        LOG.info("Creating Siddhi Query");
        ExecutionContext context = ExecutionContext.getContext();
        this.graph = context.getGraph();
        for (Iterator rootBundleIterator = context.getRootBundles().iterator(); rootBundleIterator.hasNext(); ) {
            CommittedBundle<SourceWrapper> rootBundle = (CommittedBundle) rootBundleIterator.next();
            if (!rootBundle.getPCollection().getName().equals("Readfile/Read.out")) {
                continue;
            }
            List<AppliedPTransform<?, ?, ?>> transformList = graph.getPerElementConsumers(rootBundle.getPCollection());
            for (Iterator transformIterator = transformList.iterator(); transformIterator.hasNext(); ) {
                AppliedPTransform transform = (AppliedPTransform) transformIterator.next();
                if (SiddhiApp.compatibleTransform(transform.getTransform())) {
                    generateSiddhiQueryForTransform(transform, rootBundle.getPCollection());
                }
            }
        }
    }

    public void createSiddhiRuntime() {
        LOG.info("Creating Siddhi Runtime");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "";
        String queries = "";
        for (Iterator iter = this.streamDefinitions.iterator(); iter.hasNext(); ) {
            streams = streams + iter.next().toString();
        }
        for (Iterator iter = this.queryDefinitions.iterator(); iter.hasNext(); ) {
            queries = queries + iter.next().toString();
        }
        System.out.println(streams + queries);
        siddhiManager.setExtension("beam:execute", BeamStreamProcessor.class);
        siddhiManager.setExtension("beam:groupbykey", GroupByKeyProcessor.class);
        this.runtime = siddhiManager.createSiddhiAppRuntime(streams + queries);

        runtime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for ( int i=0; i<events.length; i++ ) {
                    Event event = events[i];
                    SiddhiApp.this.bundle.addItem(event.getData()[0]);
                }
            }
        });
    }

    private void generateSiddhiQueryForTransform(AppliedPTransform transform, PCollection keyCollection) {
        /*
        If transform is not in HashMap
         */
        if (this.transformsMap.get(transform.getFullName()) == null) {
            /*
            Add stream definition and to HashMap for given transform
             */
            String streamName = SiddhiApp.stringTransform(transform.getFullName()) + "Stream";
            String sink = "";
            String stream = sink + "define stream " + streamName + " (event object);";
            this.streamDefinitions.add(stream);
            this.transformsMap.put(SiddhiApp.stringTransform(transform.getFullName()), transform);
            this.collectionsMap.put(SiddhiApp.stringTransform(transform.getFullName()), keyCollection);
            String finalOutputStream = "outputStream";

            /*
            Create queries for each transform mapped by output collections
             */

            for ( Iterator transformOuputIterator = transform.getOutputs().values().iterator(); transformOuputIterator.hasNext(); ) {
                PCollection collection = (PCollection) transformOuputIterator.next();
                List<AppliedPTransform<?, ?, ?>> transformList = this.graph.getPerElementConsumers(collection);
                for (Iterator transformListIterator = transformList.iterator(); transformListIterator.hasNext(); ) {
                    AppliedPTransform nextTransform = (AppliedPTransform) transformListIterator.next();
                    String outputStreamName;
                    if (SiddhiApp.compatibleTransform(nextTransform.getTransform())) {
                        outputStreamName = SiddhiApp.stringTransform(nextTransform.getFullName()) + "Stream";
                    } else {
                        LOG.info("Siddhi does not support " + nextTransform.getTransform().toString() + " at the moment");
                        if (this.finalCollection == null) {
                            this.finalCollection = collection;
                        }
                        outputStreamName = finalOutputStream;
                    }
                    if (transform.getTransform() instanceof ParDo.MultiOutput) {
                        String query = "from " + streamName + "#beam:execute(event, \"" + SiddhiApp.stringTransform(transform.getFullName()) + "\") " +
                                "select event insert into " + outputStreamName + ";";
                        this.queryDefinitions.add(query);
                    }
                    if (transform.getTransform() instanceof GroupByKey) {
                        String query = "from " + streamName + "#beam:groupbykey(event) " +
                                "select event insert into " + outputStreamName + ";";
                        this.queryDefinitions.add(query);
                    }
                    if (transform.getTransform() instanceof Window.Assign) {
                        String query = "";
                        Window.Assign windowTransform = (Window.Assign) transform.getTransform();
                        if (windowTransform.getWindowFn() instanceof FixedWindows) {
                            FixedWindows fixedWindow = (FixedWindows) windowTransform.getWindowFn();
                            Duration size = fixedWindow.getSize();
                            Duration offSet = fixedWindow.getOffset();
                            query += "from " + streamName + "#window.timeBatch(" + size.getStandardSeconds() + " sec";
                            if (offSet == Duration.ZERO) {
                                query += ")";
                            } else {
                                query += ", " + offSet.getStandardSeconds() + " sec)";
                            }
                            query += " select event insert into " + outputStreamName + ";";
                        }
                        if (windowTransform.getWindowFn() instanceof GlobalWindows) {
                            query = "from " + streamName + "#window.timeBatch(1 sec) select event insert into " + outputStreamName + ";";
                        }
                        this.queryDefinitions.add(query);
                    }
                    if (outputStreamName != finalOutputStream) {
                        generateSiddhiQueryForTransform(nextTransform, collection);
                    }
                }
            }
        }
    }

    public static boolean compatibleTransform(PTransform transform) {
        if (transform instanceof ParDo.MultiOutput) {
            return true;
        }
        if (transform instanceof GroupByKey) {
            return true;
        }
        if (transform instanceof Window.Assign) {
            if (((Window.Assign) transform).getWindowFn() instanceof FixedWindows) {
                return true;
            }
            if (((Window.Assign) transform).getWindowFn() instanceof GlobalWindows) {
                return true;
            }
        }
        return false;
    }

    public void setBundle(CommittedBundle bundle) {
        this.bundle = bundle;
    }

    public CommittedBundle getBundle() {
        return this.bundle;
    }

    public SiddhiAppRuntime getSiddhiRuntime() {
        return this.runtime;
    }

    public HashMap<String, AppliedPTransform> getTransformsMap() {
        return this.transformsMap;
    }

    public HashMap<String, PCollection> getCollectionsMap() {
        return this.collectionsMap;
    }

    public PCollection getFinalCollection() {
        return this.finalCollection;
    }

    public static String stringTransform(String value) {
        return value.replace('/', '_').replace('(', '_').replace(")", "").replace('.','_');
    }

}
