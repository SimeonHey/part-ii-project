import com.codahale.metrics.Meter;

import java.util.HashMap;

public class NamedMeterMeasurements {
    private final String rootName;
    private final HashMap<String, Meter> metersMap = new HashMap<>();

    public NamedMeterMeasurements(String rootName) {
        this.rootName = rootName;
    }

    public void markFor(String resource) {
        if (!metersMap.containsKey(resource)) {
            metersMap.put(resource, Constants.METRIC_REGISTRY.meter(rootName + "." + resource));
        }

        metersMap.get(resource).mark();
    }
}
