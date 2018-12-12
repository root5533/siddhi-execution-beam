package org.wso2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.wso2.beam.runner.siddhi.SiddhiRunner;

import java.util.Arrays;
import java.util.Iterator;


public class FlattenWithWindow
{

    private interface CSVOptions extends PipelineOptions, StreamingOptions {
        @Description("Set output target")
        @Default.String("/Users/admin/Projects/siddhi-execution-beam/outputs/groupByKeySiddhiResult")
        String getOutput();
        void setOutput(String value);

    }

    private static class CheckElement extends DoFn<String, KV<String, String[]>> {

        String[] regions = {"Europe", "Asia", "Middle East and North Africa", "Central America and the Caribbean", "Australia and Oceania", "Sub-Saharan Africa"};

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<KV<String, String[]>> out) {
            String[] words = element.split(",");
            if (Arrays.asList(regions).contains(words[0].trim())) {
                KV<String, String[]> kv = KV.of(words[0].trim(), Arrays.copyOfRange(words, 1, words.length));
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
        CSVOptions options = PipelineOptionsFactory.fromArgs(args).as(CSVOptions.class);
        options.setRunner(SiddhiRunner.class);
        runCSVDemo(options);
    }

    private static void runCSVDemo(CSVOptions options) {

        Pipeline pipe = Pipeline.create(options);

        PCollection<KV<String, String[]>> collection_1 = pipe.apply("Readfile", TextIO.read().from("/home/tuan/WSO2/inputs/input-small.csv"))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply(new CSVFilterRegion());

        PCollection<KV<String, String[]>> collection_2 = pipe.apply("Readfile2", TextIO.read().from("/home/tuan/WSO2/inputs/test-input.csv"))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply(new CSVFilterRegion());

        PCollectionList<KV<String, String[]>> collectionList = PCollectionList.of(collection_1).and(collection_2);

        PCollection<KV<String, String[]>> merged = collectionList.apply(Flatten.pCollections());
        merged.apply(GroupByKey.create()).apply(MapElements.via(new FindKeyValueFn()))
            .apply(TextIO.write().to(options.getOutput()));

        pipe.run();
    }
}
