import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;

import java.util.concurrent.TimeUnit;

public class Constants {
    public static final String KAFKA_TOPIC = "transactions";
    public static final int KAFKA_CONSUME_DELAY_MS = 1;
    public static final int KAFKA_DEFAULT_PARTITION = 0;

    public static final Addressable NO_RESPONSE = new Addressable("dontrespond", 0L);

    public static final long STORAGE_SYSTEMS_POLL_TIMEOUT = 30;
    public static final TimeUnit STORAGE_SYSTEMS_POLL_UNIT = TimeUnit.SECONDS;

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    public static final Gson gson = new Gson();

    public static String getStorageSystemBaseName(String fullName) {
        return fullName.split(" ")[0];
    }
}
