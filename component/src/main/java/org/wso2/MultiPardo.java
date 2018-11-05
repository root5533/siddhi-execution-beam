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

public class MultiPardo {

    private interface SiddhiOptions extends SiddhiPipelineOptions, StreamingOptions {
        @Description("Set input target")
        @Default.String("/siddhi-beam.txt")
        String getInputFile();
        void setInputFile(String value);

        @Description("Set output target")
        @Default.String("/Users/admin/Projects/siddhi-execution-beam/outputs/result")
        String getOutput();
        void setOutput(String value);
    }

    private static class SplitString extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {
            String[] words = element.split(" ");
            for (int i=0; i<words.length; i++) {
                out.output(words[i]);
            }
        }
    }

    private static class LetterCount extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {
            int count = element.length();
            String output = String.valueOf(count);
            out.output(output);
        }
    }

    private static void runSimpleSiddhiApp(SiddhiOptions options) {
        Pipeline pipe = Pipeline.create(options);
        PCollection<String> col1 = pipe.apply("Readfile", TextIO.read().from(options.getInputFile()));
        PCollection<String> col2 = col1.apply("SplitString", ParDo.of(new SplitString()));
        PCollection<String> col3 = col2.apply("PardoTransform", ParDo.of(new LetterCount()));
        col3.apply("Writefile", TextIO.write().to(options.getOutput()));
        pipe.run();
    }

    public static void main(String[] args) {
        SiddhiOptions options = PipelineOptionsFactory.fromArgs(args).as(SiddhiOptions.class);
        options.setRunner(SiddhiRunner.class);
        runSimpleSiddhiApp(options);
    }

}
