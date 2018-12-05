package org.wso2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.wso2.beam.runner.siddhi.SiddhiPipelineOptions;
import org.wso2.beam.runner.siddhi.SiddhiRunner;

public class DualParDo {

    private interface SiddhiOptions extends SiddhiPipelineOptions, StreamingOptions {
        @Description("Set input target")
        @Validation.Required
        String getInputFile();
        void setInputFile(String value);

        @Description("Set output target")
        @Default.String("/outputs/result")
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        SiddhiOptions options = PipelineOptionsFactory.fromArgs(args).as(SiddhiOptions.class);
        options.setRunner(SiddhiRunner.class);
        runDualParDOBeamApp(options);
    }

    private static void runDualParDOBeamApp(SiddhiOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Readfile", TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new StringToArray()))
                .apply(ParDo.of(new ArrayFunc()))
                .apply("Writefile", TextIO.write().to(options.getOutput()));
        pipeline.run();
    }

    private static class StringToArray extends DoFn<String, String[]> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String[]> out) {
            String[] dataArray = element.split(",");
            out.output(dataArray);
        }
    }

    private static class ArrayFunc extends DoFn<String[], String> {
        @ProcessElement
        public void processElement(@Element String[] element, OutputReceiver<String> out) {
            float totalSum = Float.parseFloat(element[element.length - 1]) + Float.parseFloat(element[element.length - 2]) + Float.parseFloat(element[element.length - 3]);
            String finalResult = "Country : " + element[1] + " Item : " + element[2] + " Total : " + String.valueOf(totalSum);
            out.output(finalResult);
        }
    }

}
