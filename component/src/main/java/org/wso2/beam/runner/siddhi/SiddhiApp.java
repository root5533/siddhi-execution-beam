package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupByKey;
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
import org.wso2.extension.siddhi.execution.beam.streamprocessor.SourceSinkProcessor;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import java.util.*;
import java.lang.reflect.Method;

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
    private String writeStreamName = "writeStream";
//    private String finalStream = "outputStream";

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
                if (SiddhiApp.compatibleTransform(transform)) {
                    generateSiddhiQueryForTransform(transform, rootBundle.getPCollection());
                }
            }
        }

        /*
        Create final writeStream siddhi query(hardcoded)
         */
//        for (Iterator iterator = this.graph.getAllPerElementConsumers().asMap().values().iterator(); iterator.hasNext(); ) {
//            List transformList = (List) iterator.next();
//            AppliedPTransform transform = (AppliedPTransform) transformList.get(0);
//            if (transform.getFullName().equals("Writefile/WriteFiles/FinalizeTempFileBundles/Finalize/ParMultiDo(Finalize)")) {
//                String stream = "define stream writeStream (event object);";
//                this.streamDefinitions.add(stream);
//                this.transformsMap.put(SiddhiApp.generateTransformName(transform.getFullName()), transform);
//                for (Iterator iter = transform.getInputs().values().iterator(); iter.hasNext();) {
//                    PCollection collection = (PCollection) iter.next();
//                    this.collectionsMap.put(SiddhiApp.generateTransformName(transform.getFullName()), collection);
//                    break;
//                }
//                String query = "from " + this.writeStreamName + "#beam:execute(event, \"" + SiddhiApp.generateTransformName(transform.getFullName()) + "\") " +
//                        "select event insert into " + this.finalStream;
//                this.queryDefinitions.add(query);
//            }
//        }
//        generateSinkQuery("text");

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
        siddhiManager.setExtension("beam:sourcesink", SourceSinkProcessor.class);
        this.runtime = siddhiManager.createSiddhiAppRuntime(streams + queries);

//        runtime.addCallback("outputStream", new StreamCallback() {
//            @Override
//            public void receive(Event[] events) {
//            for ( int i=0; i<events.length; i++ ) {
//                Event event = events[i];
//                SiddhiApp.this.bundle.addItem(event.getData()[0]);
//            }
//            }
//        });
    }

    private void generateSiddhiQueryForTransform(AppliedPTransform transform, PCollection keyCollection) {
        /*
        If transform is not in HashMap
         */
        if (this.transformsMap.get(transform.getFullName()) == null) {
            /*
            Add stream definition and to HashMap for given transform
             */
            String streamName = SiddhiApp.generateTransformName(transform.getFullName()) + "Stream";
            String sink = "";
            String stream = sink + "define stream " + streamName + " (event object);";
            this.streamDefinitions.add(stream);
            this.transformsMap.put(SiddhiApp.generateTransformName(transform.getFullName()), transform);
            this.collectionsMap.put(SiddhiApp.generateTransformName(transform.getFullName()), keyCollection);

            /*
            Create queries for each transform mapped by output collections
             */
            boolean duplicateQueryFlag = false;
            if (transform.getTransform() instanceof TextIO.Write) {
                try {
                    TextIO.TypedWrite textio = ((TextIO.Write) transform.getTransform()).withOutputFilenames();
                    Class cls = textio.getClass();
                    Method getFileNamePrefix = cls.getDeclaredMethod("getFilenamePrefix");
                    getFileNamePrefix.setAccessible(true);
                    ValueProvider.StaticValueProvider provider = (ValueProvider.StaticValueProvider) getFileNamePrefix.invoke(textio);
                    String filePath = provider.get().toString();
                    generateSinkQuery("text", streamName, filePath, false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                for (Iterator transformOuputIterator = transform.getOutputs().values().iterator(); transformOuputIterator.hasNext(); ) {
                    PCollection collection = (PCollection) transformOuputIterator.next();
                    List<AppliedPTransform<?, ?, ?>> transformList = this.graph.getPerElementConsumers(collection);
                    for (Iterator transformListIterator = transformList.iterator(); transformListIterator.hasNext(); ) {
                        AppliedPTransform nextTransform = (AppliedPTransform) transformListIterator.next();
                        String outputStreamName;
                        if (SiddhiApp.compatibleTransform(nextTransform)) {
                            outputStreamName = SiddhiApp.generateTransformName(nextTransform.getFullName()) + "Stream";
                        } else {
                            LOG.info("Siddhi does not support " + nextTransform.getTransform().toString() + " at the moment");
                            if (duplicateQueryFlag == true) {
                                continue;
                            } else {
                                duplicateQueryFlag = true;
                            }
                            if (this.finalCollection == null) {
                                this.finalCollection = collection;
                            }
                            outputStreamName = this.writeStreamName;
                        }
                        if (transform.getTransform() instanceof ParDo.MultiOutput) {
                            String query = "from " + streamName + "#beam:execute(event, \"" + SiddhiApp.generateTransformName(transform.getFullName()) + "\") " +
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
                                //                            query = "from " + streamName + " select event insert into " + outputStreamName + ";";
                            }
                            this.queryDefinitions.add(query);
                        }
                        if (outputStreamName != this.writeStreamName) {
                            generateSiddhiQueryForTransform(nextTransform, collection);
                        }
                    }
                }
            }
        }
    }

    public static boolean compatibleTransform(AppliedPTransform transform) {
        if (transform.getTransform() instanceof ParDo.MultiOutput) {
            if (transform.getFullName().equals("Writefile/WriteFiles/WriteUnshardedBundlesToTempFiles/WriteUnshardedBundles")) {
                return false;
            }
            return true;
        }
        if (transform.getTransform() instanceof GroupByKey) {
            if (transform.getFullName().equals("Writefile/WriteFiles/WriteUnshardedBundlesToTempFiles/GroupUnwritten")) {
                return false;
            }
            return true;
        }
        if (transform.getTransform() instanceof Window.Assign) {
            if (((Window.Assign) transform.getTransform()).getWindowFn() instanceof FixedWindows) {
                return true;
            }
            if (((Window.Assign) transform.getTransform()).getWindowFn() instanceof GlobalWindows) {
                return true;
            }
        }
        if (transform.getTransform() instanceof TextIO.Write) {
            return true;
        }
        return false;
    }

    public void setBundle(CommittedBundle bundle) {
        this.bundle = bundle;
    }

    private void generateSinkQuery(String sinkType, String streamName, String filePath, boolean windowedWrite) {
        if (sinkType.equals("text")) {
            /*
            Extract string value from WindowedValue object and pass to sink stream
             */
            String sinkStreamName = "textSinkStream";
            String query = "from " + streamName + "#beam:sourcesink(event) select value insert into " + sinkStreamName + ";";
            this.queryDefinitions.add(query);

            /*
            Define sink query
             */
            if (windowedWrite == false) {
                String sink = "@sink(type='file', file.uri='" + filePath + "', append='true', @map(type='text', @payload('{{value}}') ))";
                String textSinkStream = sink + " define stream " + sinkStreamName + " (value string);";
                this.streamDefinitions.add(textSinkStream);
            }
        }
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

    public static String generateTransformName(String value) {
        return value.replace('/', '_').replace('(', '_').replace(")", "").replace('.','_');
    }

}
