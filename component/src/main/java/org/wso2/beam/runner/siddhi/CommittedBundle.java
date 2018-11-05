package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.values.PCollection;

import java.util.LinkedList;
import java.util.Queue;

public class CommittedBundle<T> {

    private PCollection bundle;
    private Queue<T> values = new LinkedList<>();

    public CommittedBundle(PCollection bundle) {
        this.bundle = bundle;
    }

    public void addItem(T item) {
        values.add(item);
    }

    public PCollection getPCollection() {
        return this.bundle;
    }

    public void setPCollection(PCollection bundle) {
        this.bundle = bundle;
    }

    public SourceWrapper getSourceWrapper() {
        return (SourceWrapper) this.values.poll();
    }

    public Queue getValues() {
        return this.values;
    }

}
