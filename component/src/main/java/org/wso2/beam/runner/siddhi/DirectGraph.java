package org.wso2.beam.runner.siddhi;


import com.google.common.collect.ListMultimap;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DirectGraph implements ExecutableGraph<AppliedPTransform<?, ?, ?>, PValue> {

    private final Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers;
    private final ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers;
    private final Set<AppliedPTransform<?, ?, ?>> rootTransforms;
    private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;

    public static DirectGraph create(Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers, ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
                                     Set<AppliedPTransform<?, ?, ?>> rootTransforms, Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
        return new DirectGraph(producers, perElementConsumers, rootTransforms, stepNames);
    }

    private DirectGraph(Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers, ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
                        Set<AppliedPTransform<?, ?, ?>> rootTransforms, Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
        this.producers = producers;
        this.perElementConsumers = perElementConsumers;
        this.rootTransforms = rootTransforms;
        this.stepNames = stepNames;
    }

    public Set<AppliedPTransform<?, ?, ?>> getRootTransforms() {
        return this.rootTransforms;
    }

    public List<AppliedPTransform<?, ?, ?>> getPerElementConsumers(PValue consumed) {
        return this.perElementConsumers.get(consumed);
    }

    public ListMultimap getAllPerElementConsumers() {
        return this.perElementConsumers;
    }

}
