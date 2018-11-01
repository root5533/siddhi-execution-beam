package org.wso2.beam.runner.siddhi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.execution.beam.streamprocessor.BeamStreamProcessor;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class SiddhiApp {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiApp.class);
    private SiddhiAppRuntime runtime;
    private CommittedBundle bundle;

    public SiddhiApp() {
        LOG.info("Creating Siddhi Runtime");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream inputStream (event object);";
        String query = "from inputStream#beam:execute(event) select event insert into outputStream";
        siddhiManager.setExtension("beam:execute", BeamStreamProcessor.class);
        this.runtime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

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

    public void setBundle(CommittedBundle bundle) {
        this.bundle = bundle;
    }

    public CommittedBundle getBundle() {
        return this.bundle;
    }

    public SiddhiAppRuntime getSiddhiRuntime() {
        return this.runtime;
    }

}
