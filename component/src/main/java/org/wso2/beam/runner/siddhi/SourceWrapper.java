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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SourceWrapper<OutputT> {

    private static final Logger log = LoggerFactory.getLogger(SourceWrapper.class);
    private List<? extends BoundedSource<OutputT>> splitSources;
    private boolean isOpen = false;
    private PipelineOptions options;
    private List<BoundedSource<OutputT>> localSplitSources;
    private List<BoundedReader<OutputT>> localReaders;
    private List<Event> elements = new ArrayList<>();

    SourceWrapper(BoundedSource source, int parallelism, PipelineOptions options) throws Exception {
        this.options = options;
        long estimatedBytes = source.getEstimatedSizeBytes(options);
        long bytesPerBundle = estimatedBytes / (long) parallelism;
        this.splitSources = source.split(bytesPerBundle, options);
        this.localSplitSources = new ArrayList<>();
        this.localReaders = new ArrayList<>();
    }

    void open() throws IOException {
        this.isOpen = true;
        for (BoundedSource<OutputT> source: this.splitSources) {
            BoundedReader<OutputT> reader = source.createReader(this.options);
            this.localSplitSources.add(source);
            this.localReaders.add(reader);
        }
    }

    void run(InputHandler inputHandler) {

        /*
         Run the source to emit each element to DoFnOperator delegate
         */
        try {
            for (BoundedReader<OutputT> reader: this.localReaders) {
                boolean hasData = reader.start();
                while (hasData) {
//                this.emitElement(inputHandler, reader);
                    WindowedValue elem = WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp());
                    this.convertToEvent(elem);
                    hasData = reader.advance();
                }
            }
            this.emitElements(inputHandler);
        } catch (InterruptedException exception) {
            log.error("Interrupted Exception : ", exception.getMessage());
        } catch (IOException exception) {
            log.error("IOException", exception.getMessage());
        }

    }

//    private void emitElement (InputHandler inputHandler, BoundedReader reader) throws Exception {
//        WindowedValue elem = WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp());
//        inputHandler.send(new Object[]{elem});
//    }

    private void emitElements(InputHandler inputHandler) throws InterruptedException {
        Event[] stream = elements.toArray(new Event[0]);
        inputHandler.send(stream);
    }

    private void convertToEvent(WindowedValue elem) {
        Event event = new Event();
        event.setData(new Object[]{elem});
        elements.add(event);
    }

}
