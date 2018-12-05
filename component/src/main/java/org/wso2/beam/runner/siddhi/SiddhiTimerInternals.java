package org.wso2.beam.runner.siddhi;

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.state.TimeDomain;
import org.joda.time.Instant;

public class SiddhiTimerInternals implements TimerInternals {

    @Override
    public void setTimer(StateNamespace namespace, String timerId, Instant target, TimeDomain timeDomain) {
        System.out.println("set timer 1");
    }

    @Override
    public void setTimer(TimerData timerData) {
        System.out.println("set timer 2");
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
        System.out.println("delete timer 1");
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId) {
        System.out.println("delete timer 2");
    }

    @Override
    public void deleteTimer(TimerData timerKey) {
        System.out.println("delete timer 3");
    }

    @Override
    public Instant currentProcessingTime() {
        System.out.println("current processing timer");
        return null;
    }

    @Override
    public Instant currentSynchronizedProcessingTime() {
        System.out.println("current synchronized processing time");
        return null;
    }

    @Override
    public Instant currentInputWatermarkTime() {
        System.out.println("current input watermark time");
        return null;
    }

    @Override
    public Instant currentOutputWatermarkTime() {
        System.out.println("current output watermark time");
        return null;
    }
}
