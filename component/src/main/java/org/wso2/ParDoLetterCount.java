package org.wso2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.wso2.beam.runner.siddhi.SiddhiPipelineOptions;
import org.wso2.beam.runner.siddhi.SiddhiRunner;

public class ParDoLetterCount {

    private static class LetterCount extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {
            int count = element.length();
            String output = String.valueOf(count);
            out.output(output);
        }
    }

    private static void runSimpleSiddhiApp(SiddhiPipelineOptions options) {
        Pipeline pipe = Pipeline.create(options);
        PCollection<String> col1 = pipe.apply("Readfile", TextIO.read().from(options.getInputFile()));
        PCollection<String> col2 = col1.apply("PardoTransform", ParDo.of(new LetterCount()));
        col2.apply("Writefile", TextIO.write().to(options.getOutput()));
        pipe.run();
    }

    public static void main(String[] args) {
        SiddhiPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(SiddhiPipelineOptions.class);
        options.setRunner(SiddhiRunner.class);
        runSimpleSiddhiApp(options);
    }

}
