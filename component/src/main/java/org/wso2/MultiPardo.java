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

import java.util.Arrays;
import java.util.List;

public class MultiPardo {

    private static class SplitString extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {
            String[] words = element.split(" ");
            for (int i=0; i<words.length; i++) {
                out.output(words[i]);
            }
        }
    }

    private static class FilterString extends DoFn<String, String> {

        String[] filters = {"the", "a", "is", "of", "has"};
        List<String> filterList = Arrays.asList(filters);

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {
            if (!filterList.contains(element)) {
                out.output(element);
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

    private static void runSimpleSiddhiApp(SiddhiPipelineOptions options) {
        Pipeline pipe = Pipeline.create(options);
        PCollection<String> col1 = pipe.apply("Readfile", TextIO.read().from(options.getInputFile()));
        PCollection<String> col2 = col1.apply("SplitString", ParDo.of(new SplitString()));
        PCollection<String> col3 = col2.apply("FilterString", ParDo.of(new FilterString()));
        PCollection<String> col4 = col3.apply("LetterCount", ParDo.of(new LetterCount()));
        col4.apply("Writefile", TextIO.write().to(options.getOutput()));
        pipe.run();
    }

    public static void main(String[] args) {
        SiddhiPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(SiddhiPipelineOptions.class);
        options.setRunner(SiddhiRunner.class);
        runSimpleSiddhiApp(options);
    }

}
