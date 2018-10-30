package org.wso2.beam.localrunner;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Queue;

public class ExecutionContext {

    private final DirectGraph graph;
    private HashMap<PCollection, CommittedBundle> bundles = new HashMap<>();
    private HashMap<PCollection, CommittedBundle> rootBundles = new HashMap<>();
    private Iterator rootBundlesIterator;

    public ExecutionContext(DirectGraph graph) {
        this.graph = graph;
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

}
