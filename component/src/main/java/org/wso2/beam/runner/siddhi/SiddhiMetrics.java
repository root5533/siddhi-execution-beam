package org.wso2.beam.runner.siddhi;

import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;

/**
 *
 */
public class SiddhiMetrics extends MetricResults {

    @Override
    public MetricQueryResults queryMetrics(MetricsFilter filter) {
        throw new UnsupportedOperationException("stateInternals is not supported");
    }
}
