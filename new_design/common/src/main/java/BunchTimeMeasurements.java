import java.util.HashMap;

public class BunchTimeMeasurements {
    private final String rootName;
    private final HashMap<String, TimeMeasurement> timeMeasurements = new HashMap<>();

    public BunchTimeMeasurements(String rootName) {
        this.rootName = rootName;
    }

    private TimeMeasurement getOrSet(String resource) {
        if (timeMeasurements.containsKey(resource)) {
            return timeMeasurements.get(resource);
        }

        TimeMeasurement timeMeasurement = new TimeMeasurement(Constants.METRIC_REGISTRY,
            String.format("%s.%s", rootName, resource));
        timeMeasurements.put(resource, timeMeasurement);
        return timeMeasurement;
    }

    public void measureTime(String resource, Runnable operation) {
        getOrSet(resource).measureAndPublish(operation);
    }

    public void startTimer(String resource) {
        getOrSet(resource).startTimer();
    }

    public void stopTimerAndPublish(String resource) {
        getOrSet(resource).stopTimerAndPublish();
    }
}
