package org.wso2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.wso2.beam.runner.siddhi.SiddhiPipelineOptions;
import org.wso2.beam.runner.siddhi.SiddhiRunner;

import java.util.Arrays;
import java.util.Iterator;


public class GroupingByKey
{

    private static class CheckElement extends DoFn<String, KV<String, String[]>> {

        String[] regions = {"Europe", "Asia", "Middle East and North Africa", "Central America and the Caribbean", "Australia and Oceania", "Sub-Saharan Africa"};

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<KV<String, String[]>> out) {
            String[] words = element.split(",");
            if (Arrays.asList(regions).contains(words[0].trim())) {
                KV<String, String[]> kv = KV.of(words[0], Arrays.copyOfRange(words, 1, words.length));
                out.output(kv);
            }
        }

    }

    public static class FindKeyValueFn extends SimpleFunction<KV<String, Iterable<String[]>>, String> {

        @Override
        public String apply(KV<String, Iterable<String[]>> input) {
            Iterator<String[]> iter = input.getValue().iterator();
            float total_profit = 0;
            while (iter.hasNext()) {
                String[] details = iter.next();
                total_profit += Float.parseFloat(details[details.length - 1]) / 1000000;
            }
            String result = input.getKey().trim() + " region profits : $ " + total_profit + " Million";
            return result;
        }

    }

    private static class CSVFilterRegion extends PTransform<PCollection<String>, PCollection<KV<String, String[]>>> {

        public PCollection<KV<String, String[]>> expand(PCollection<String> lines) {
            PCollection<KV<String, String[]>> filtered = lines.apply(ParDo.of(new CheckElement()));
            return filtered;
        }

    }

    public static void main( String[] args )
    {
        SiddhiPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(SiddhiPipelineOptions.class);
        options.setRunner(SiddhiRunner.class);
        runCSVDemo(options);
    }

    private static void runCSVDemo(SiddhiPipelineOptions options) {

        Pipeline pipe = Pipeline.create(options);
        pipe.apply("Readfile", TextIO.read().from(options.getInputFile()))
                .apply(new CSVFilterRegion())
                .apply(GroupByKey.<String, String[]>create())
                .apply(MapElements.via(new FindKeyValueFn()))
                .apply("Writefile", TextIO.write().to(options.getOutput()));
        pipe.run();
    }
}
