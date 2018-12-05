package org.wso2.beam.runner.siddhi;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;

public class SiddhiStateInternals implements StateInternals {
    @Override
    public Object getKey() {
        System.out.println("stateinternals getkey");
        return null;
    }

    @Override
    public <T extends State> T state(StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
        System.out.println("stateinternals state");
        return null;
    }
}
