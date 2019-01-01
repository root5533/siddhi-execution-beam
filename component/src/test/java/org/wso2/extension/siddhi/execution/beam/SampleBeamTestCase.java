package org.wso2.extension.siddhi.execution.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.beam.runner.siddhi.SiddhiPipelineOptions;
import org.wso2.beam.runner.siddhi.SiddhiRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SampleBeamTestCase {

    File sinkRoot;
    String source, sink;

    @BeforeClass
    public void init() {
        String workingdir = System.getProperty("user.dir");
        System.out.println("system property path" + workingdir);
        ClassLoader classLoader = SampleBeamTestCase.class.getClassLoader();
        String rootPath = classLoader.getResource("files").getFile();
        source = rootPath + "/parDo/sample.txt";
        sink = rootPath = "/parDo/resultSample.txt";
//        sinkRoot = new File(rootPath + "/parDo");
    }

    private static class SplitString extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {
            String[] words = element.split(" ");
            for (String word : words) {
                out.output(word);
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

    private void runSimpleSiddhiApp(SiddhiPipelineOptions options) {
        Pipeline pipe = Pipeline.create(options);
        PCollection<String> col1 = pipe.apply(TextIO.read().from(source));
        PCollection<String> col2 = col1.apply(ParDo.of(new SampleBeamTestCase.SplitString()));
        PCollection<String> col3 = col2.apply(ParDo.of(new SampleBeamTestCase.FilterString()));
        PCollection<String> col4 = col3.apply(ParDo.of(new SampleBeamTestCase.LetterCount()));
        col4.apply(TextIO.write().to(sink));
        pipe.run();
    }

    @Test
    public void SampleTest() throws InterruptedException {
        SiddhiPipelineOptions options = PipelineOptionsFactory.as(SiddhiPipelineOptions.class);
        options.setRunner(SiddhiRunner.class);
        runSimpleSiddhiApp(options);
        Thread.sleep(1000);

        int lineLength[] = {5, 5, 6, 6};
        File sinkFile = new File(sink);
        try {
            if (sinkFile.isFile()) {
                BufferedReader reader = new BufferedReader(new FileReader(sinkFile));
                String line;
                int i = 0;
                while ((line = reader.readLine()) != null) {
                    AssertJUnit.assertEquals(Integer.parseInt(line), lineLength[i]);
                    i++;
                }
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + sinkFile.getAbsolutePath());
        }
    }

}
