package org.wso2.extension.siddhi.execution.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
import org.testng.TestException;
import org.testng.annotations.AfterMethod;
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

public class MultiParDoTestCase {

    String rootPath, source, sink;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = MultiParDoTestCase.class.getClassLoader();
        rootPath = classLoader.getResource("files").getFile();
        source = rootPath + "/parDo/sample.txt";
        sink = rootPath + "/sink/resultSample.txt";
    }

    @AfterMethod
    public void doAfterMethod() {
        try {
            FileUtils.deleteDirectory(new File(rootPath + "/sink"));
        } catch (IOException e) {
            throw new TestException("Failed to delete files in due to " + e.getMessage(), e);
        }
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

    private void runMultiParDo(SiddhiPipelineOptions options) {
        Pipeline pipe = Pipeline.create(options);
        PCollection<String> col1 = pipe.apply(TextIO.read().from(source));
        PCollection<String> col2 = col1.apply(ParDo.of(new MultiParDoTestCase.SplitString()));
        PCollection<String> col3 = col2.apply(ParDo.of(new MultiParDoTestCase.FilterString()));
        PCollection<String> col4 = col3.apply(ParDo.of(new MultiParDoTestCase.LetterCount()));
        col4.apply(TextIO.write().to(sink));
        pipe.run();
    }

    @Test
    public void multiParDoTest() throws InterruptedException {
        SiddhiPipelineOptions options = PipelineOptionsFactory.as(SiddhiPipelineOptions.class);
        options.setRunner(SiddhiRunner.class);
        runMultiParDo(options);
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
            } else {
                AssertJUnit.fail(sink + " is not a directory");
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + sinkFile.getAbsolutePath());
        }
    }

}
