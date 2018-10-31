package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import java.util.*;

public class ExecutionContext {

    private DirectGraph graph;
    private HashMap<PCollection, CommittedBundle> bundles = new HashMap<>();
    private HashMap<PCollection, CommittedBundle> rootBundles = new HashMap<>();
    private Iterator rootBundlesIterator;
    private static final ExecutionContext context = new ExecutionContext();
    private AppliedPTransform startTransform;

    private ExecutionContext() {}

    public void setGraph(DirectGraph graph) {
        this.graph = graph;
    }

    public static ExecutionContext getContext() {
        return context;
    }

    public void addRootBundle(CommittedBundle bundle) {
        this.bundles.put(bundle.getPCollection(), bundle);
        this.rootBundles.put(bundle.getPCollection(), bundle);
    }

    public CommittedBundle getBundle(PCollection key) {
        return this.bundles.get(key);
    }

    public CommittedBundle getPendingRootBundle() {
        if (this.rootBundlesIterator == null) {
            this.rootBundlesIterator = this.rootBundles.values().iterator();
        }
        return getRootBundle();
    }

    private CommittedBundle getRootBundle() {
        return (CommittedBundle) this.rootBundlesIterator.next();
    }

    public Collection getRootBundles() {
        return rootBundles.values();
    }

    public void addOutputBundle(CommittedBundle bundle) {
        this.bundles.put(bundle.getPCollection(), bundle);
    }

    public CommittedBundle getFinalBundle() {
        for ( Iterator iter = this.bundles.values().iterator(); iter.hasNext(); ) {
            CommittedBundle currentBundle = (CommittedBundle) iter.next();
            if (currentBundle.getPCollection().getName().equals("Writefile/WriteFiles/WriteUnshardedBundlesToTempFiles/WriteUnshardedBundles.unwrittenRecords")) {
                Queue values = currentBundle.getValues();
                WindowedValue value = (WindowedValue) values.poll();
                ArrayList list = new ArrayList();
                list.add(value.getValue());
                WindowedValue newValue = WindowedValue.timestampedValueInGlobalWindow(list, value.getTimestamp());
                CommittedBundle<WindowedValue> newBundle = new CommittedBundle(currentBundle.getPCollection());
                newBundle.addItem(newValue);
                return newBundle;
            }
        }
        return null;
    }

    public void setStartTransform(AppliedPTransform transform) {
        this.startTransform = transform;
    }

    public AppliedPTransform getStartTransform() {
        return this.startTransform;
    }

}
