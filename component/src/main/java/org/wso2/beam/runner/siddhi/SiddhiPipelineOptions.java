package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface SiddhiPipelineOptions extends PipelineOptions {

    @Description("Set input target")
    @Default.String("/input")
    String getInputFile();
    void setInputFile(String value);

    @Description("Set output target")
    @Default.String("/home/tuan/WSO2/outputs/result")
    String getOutput();
    void setOutput(String value);

}
