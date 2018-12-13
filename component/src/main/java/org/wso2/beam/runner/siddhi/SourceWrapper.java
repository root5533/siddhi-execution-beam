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

package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
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
    List<Event> elements = new ArrayList<>();

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
        for (int i=0; i<this.localReaders.size(); i++) {
            BoundedReader<OutputT> reader = localReaders.get(i);
            boolean hasData = reader.start();
            while (hasData) {
//                this.emitElement(inputHandler, reader);
                WindowedValue elem = WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp());
                this.convertToEvent(elem);
                hasData = reader.advance();
            }
        }
        this.emitElements(inputHandler);

    }

//    private void emitElement (InputHandler inputHandler, BoundedReader reader) throws Exception {
//        WindowedValue elem = WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp());
//        inputHandler.send(new Object[]{elem});
//    }

    private void emitElements(InputHandler inputHandler) throws Exception {
        Event[] stream = elements.toArray(new Event[0]);
        inputHandler.send(stream);
    }

    private void convertToEvent(WindowedValue elem) {
        Event event = new Event();
        event.setData(new Object[]{elem});
        elements.add(event);
    }

}
