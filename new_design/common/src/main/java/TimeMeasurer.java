import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class TimeMeasurer {
    private final Timer timer;
    private final SettableGauge<Long> settableGauge;

    static class ActiveTimer {
        private Timer.Context timerContext;
        private long startTime;

        private ActiveTimer(Timer.Context timerContext, long startTime) {
            this.timerContext = timerContext;
            this.startTime = startTime;
        }
    }

    public TimeMeasurer(MetricRegistry registry, String resourceName) {
        this.timer = registry.timer(resourceName + ".hist");
        this.settableGauge = new SettableGauge<>();
        registry.register(resourceName + ".inst", settableGauge);
    }

    public ActiveTimer startTimer() {
        return new ActiveTimer(timer.time(), System.nanoTime());
    }

    public long stopTimerAndPublish(ActiveTimer activeTimer) {
        long elaspedMs = (System.nanoTime() - activeTimer.startTime) / 1000000;
        settableGauge.setValue(elaspedMs);
        activeTimer.timerContext.stop();

        return elaspedMs;
    }
}
