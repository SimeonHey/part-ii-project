import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class TimeMeasurement {
    private final Timer timer;
    private final SettableGauge<Long> settableGauge;

    public TimeMeasurement(MetricRegistry registry, String resourceName) {
        this.timer = registry.timer(resourceName + ".hist");
        this.settableGauge = new SettableGauge<>();
        registry.register(resourceName + ".inst", settableGauge);
    }

    public void measureAndPublish(Runnable operation) {
        long elapsed;
        try (var ignored = timer.time()) {
            long startTime = System.nanoTime();
            operation.run();
            elapsed = (System.nanoTime()- startTime) / 1000000;
            settableGauge.setValue(elapsed);
        }
    }

    private long startTime;
    private Timer.Context timerContext;

    public void startTimer() {
        this.timerContext = timer.time();
        this.startTime = System.nanoTime();
    }

    public void stopTimerAndPublish() {
        long elasped = (System.nanoTime() - startTime) / 1000000;
        settableGauge.setValue(elasped);

        this.timerContext.stop();
    }
}
