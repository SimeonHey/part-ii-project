import java.util.HashMap;
import java.util.function.Supplier;

public class NamedTimeMeasurements {
    private final String rootName;
    private final HashMap<String, TimeMeasurer> timeMeasurers = new HashMap<>();
    private final HashMap<String, HashMap<Long, TimeMeasurer.ActiveTimer>> activeTimers = new HashMap<>();

    public NamedTimeMeasurements(String rootName) {
        this.rootName = rootName;
    }

    private <K, V>V hmGetOrSet(HashMap<K, V> hm, K key, Supplier<V> supplier) {
        if (hm.containsKey(key)) {
            return hm.get(key);
        }

        V newObj = supplier.get();
        hm.put(key, newObj);
        return newObj;
    }

    private TimeMeasurer.ActiveTimer startOrGetTimer(String resource, Long uid) {
        TimeMeasurer timeMeasurer = hmGetOrSet(timeMeasurers, resource, () ->
            new TimeMeasurer(Constants.METRIC_REGISTRY, String.format("%s.%s", rootName, resource)));
        var hmTimers = hmGetOrSet(activeTimers, resource, HashMap::new);
        return hmGetOrSet(hmTimers, uid, timeMeasurer::startTimer);
    }

    public void startTimer(String resource, Long uid) {
        startOrGetTimer(resource, uid);
    }

    public void stopTimerAndPublish(String resource, Long uid) {
        var activeTimer = startOrGetTimer(resource, uid);
        timeMeasurers.get(resource).stopTimerAndPublish(activeTimer);
        activeTimers.get(resource).remove(uid);
    }
}
