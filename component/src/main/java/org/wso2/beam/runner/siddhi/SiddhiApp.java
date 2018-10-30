package org.wso2.beam.runner.siddhi;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.beam.streamprocessor.BeamStreamProcessor;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

public class SiddhiApp {

    private static final Logger LOG = Logger.getLogger(SiddhiApp.class);
    private static SiddhiAppRuntime runtime;
    private static InputHandler inputHandler;

    public static SiddhiAppRuntime create() {
        return createSiddhiAppRuntime();
    }

    private static SiddhiAppRuntime createSiddhiAppRuntime() {
        LOG.info("Creating Siddhi Runtime");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream inputStream (event string);";
        String query = "from inputStream#beam:execute(event) select event insert into outputStream";
        siddhiManager.setExtension("beam:execute", BeamStreamProcessor.class);
        runtime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
        return runtime;
    }

}
