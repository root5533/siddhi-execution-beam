package org.wso2.beam.runner.siddhi;

import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.Collections;

public class SingletonKeyedWorkItem<K, ElemT> implements KeyedWorkItem<K, ElemT> {
    final K key;
    final WindowedValue<ElemT> value;

    public SingletonKeyedWorkItem(K key, WindowedValue<ElemT> value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K key() {
        return this.key;
    }

    @Override
    public Iterable<TimerInternals.TimerData> timersIterable() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Iterable<WindowedValue<ElemT>> elementsIterable() {
        return Collections.singletonList(this.value);
    }
}
