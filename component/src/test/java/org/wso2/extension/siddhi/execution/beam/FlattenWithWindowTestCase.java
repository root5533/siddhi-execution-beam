/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.testng.annotations.Test;
import org.wso2.beam.runner.siddhi.SiddhiPipelineOptions;
import org.wso2.beam.runner.siddhi.SiddhiRunner;

import java.util.Arrays;
import java.util.Iterator;

public class FlattenWithWindowTestCase {

    private static class CheckElement extends DoFn<String, KV<String, String[]>> {

        String[] regions = {"Europe", "Asia", "Middle East and North Africa",
                "Central America", "Australia and Oceania", "Sub-Saharan Africa"};

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
            float totalProfit = 0;
            while (iter.hasNext()) {
                String[] details = iter.next();
                totalProfit += Float.parseFloat(details[details.length - 1]) / 1000000;
            }
            return input.getKey().trim() + " " + " region profits : $ " + totalProfit + " Million";
        }

    }

    private static class CSVFilterRegion extends PTransform<PCollection<String>, PCollection<KV<String, String[]>>> {

        public PCollection<KV<String, String[]>> expand(PCollection<String> lines) {
            return lines.apply(ParDo.of(new CheckElement()));
        }

    }

    @Test
    public static void flattenWithWindow() throws InterruptedException {
        SiddhiPipelineOptions options = PipelineOptionsFactory.as(SiddhiPipelineOptions.class);
        options.setRunner(SiddhiRunner.class);
        runCSVDemo(options);
        Thread.sleep(3000);
    }

    private static void runCSVDemo(SiddhiPipelineOptions options) {

        Pipeline pipe = Pipeline.create(options);

        PCollection<KV<String, String[]>> collectionOne = pipe.apply("Readfile", TextIO.read()
                .from("/home/tuan/WSO2/inputs/input-small.csv"))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))))
                .apply(new CSVFilterRegion());

        PCollection<KV<String, String[]>> collectionTwo = pipe.apply("Readfile2", TextIO.read()
                .from("/home/tuan/WSO2/inputs/test-input.csv"))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))))
                .apply(new CSVFilterRegion());

        PCollectionList<KV<String, String[]>> collectionList = PCollectionList.of(collectionOne).and(collectionTwo);

        PCollection<KV<String, String[]>> merged = collectionList.apply(Flatten.pCollections());
        merged.apply(GroupByKey.create()).apply(MapElements.via(new FindKeyValueFn()))
            .apply(TextIO.write().to(options.getOutput() + "FlattenWithWindow"));

        pipe.run();
    }
}
