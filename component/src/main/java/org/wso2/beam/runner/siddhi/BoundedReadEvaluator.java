package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.collections4.map.HashedMap;
//import org.apache.commons.collections.map.HashedMap;

import java.util.*;

public class BoundedReadEvaluator<T> {

    private final PipelineOptions options;
    private static BoundedReadEvaluator boundedReadEvaluator;
    private Map<Set<WindowedValue<T>>, String> resultBundle = new HashedMap<>();

    public static BoundedReadEvaluator create(PipelineOptions options) {
        if (boundedReadEvaluator != null) {
            return boundedReadEvaluator;
        } else {
            boundedReadEvaluator = new BoundedReadEvaluator(options);
            return boundedReadEvaluator;
        }
    }

    public static BoundedReadEvaluator getBoundedReadEvaluator() {
        return boundedReadEvaluator;
    }

    private BoundedReadEvaluator(PipelineOptions options) {
        this.options = options;
    }

    public Map<WindowedValue, AppliedPTransform<?, ?, ?>> getInitialInputs(AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform, int targetParallelism) throws Exception {
        Read.Bounded boundedInput = (Read.Bounded) transform.getTransform();
        BoundedSource<T> source = boundedInput.getSource();
        long estimatedBytes = source.getEstimatedSizeBytes(this.options);
        long bytesPerBundle = estimatedBytes / (long)targetParallelism;
        List<? extends BoundedSource<T>> bundles = source.split(bytesPerBundle, this.options);
        Map<WindowedValue, AppliedPTransform<?, ?, ?>> inputBundle = new HashedMap();
        for (Iterator iter = bundles.iterator(); iter.hasNext();) {
            inputBundle.put(WindowedValue.valueInGlobalWindow(iter.next()), transform);
        }
        return inputBundle;
    }

    public void processElement(WindowedValue<T> element) {
        try {
            BoundedSource<T> source = (BoundedSource<T>) element.getValue();
            BoundedReader<T> reader = source.createReader(this.options);
            boolean contentsRemaining = reader.start();
            Set<WindowedValue<T>> output;
            for (output = new HashSet(); contentsRemaining; contentsRemaining = reader.advance()) {
                output.add(WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp()));
            }
            resultBundle.put(output, element.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println(resultBundle.size());
        }
    }

}
