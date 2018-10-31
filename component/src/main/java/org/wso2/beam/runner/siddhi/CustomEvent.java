package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

public class CustomEvent {

    private WindowedValue element;
    private PCollection key;

    public static CustomEvent create(WindowedValue value, PCollection pCol) {
        return new CustomEvent(value, pCol);
    }

    private CustomEvent(WindowedValue value, PCollection pCol) {
        this.element = value;
        this.key = pCol;
    }

    public WindowedValue getElement() {
        return this.element;
    }

    public PCollection getPCollection() {
        return this.key;
    }

}
