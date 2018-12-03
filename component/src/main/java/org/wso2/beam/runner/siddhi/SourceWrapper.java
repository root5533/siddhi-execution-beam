package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;
import java.util.List;

public class SourceWrapper<OutputT> {

    private static final Logger LOG = LoggerFactory.getLogger(SourceWrapper.class);
    List<? extends BoundedSource<OutputT>> splitSources;
    private boolean isOpen = false;
    PipelineOptions options;
    List<BoundedSource<OutputT>> localSplitSources;
    List<BoundedReader<OutputT>> localReaders;

    public SourceWrapper(BoundedSource source, int parallelism, PipelineOptions options) throws Exception {
        this.options = options;
        long estimatedBytes = source.getEstimatedSizeBytes(options);
        long bytesPerBundle = estimatedBytes / (long) parallelism;
        this.splitSources = source.split(bytesPerBundle, options);
        this.localSplitSources = new ArrayList<>();
        this.localReaders = new ArrayList<>();
    }

    public void open() throws Exception {
        this.isOpen = true;
        for ( int i = 0; i < this.splitSources.size(); i++ ) {
            BoundedSource<OutputT> source = (BoundedSource) this.splitSources.get(i);
            BoundedReader<OutputT> reader = source.createReader(this.options);
            this.localSplitSources.add(source);
            this.localReaders.add(reader);
        }
    }

    public void run(InputHandler inputHandler) throws Exception {

        /**
         *Run the source to emit each element to DoFnOperator delegate
         */
        if (this.localReaders.size() == 1) {
            BoundedReader<OutputT> reader = localReaders.get(0);
            boolean hasData = reader.start();
            if (hasData) {
                this.emitElement(inputHandler, reader);
            }
            hasData = reader.advance();
            while (hasData) {
                this.emitElement(inputHandler, reader);
                hasData = reader.advance();
            }
        }

    }

    private void emitElement (InputHandler inputHandler, BoundedReader reader) throws Exception {
        WindowedValue elem = WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp());
        Thread.sleep(3000);
        inputHandler.send(new Object[]{elem});
    }

}
