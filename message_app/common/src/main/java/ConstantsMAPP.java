import com.google.gson.Gson;

import java.util.concurrent.TimeUnit;

public class ConstantsMAPP {

    public static final boolean DRY_RUN = false;
    public static final int VAVR_LISTEN_PORT = 8003;
    public static final int VAVR_LISTEN_PORT_ALT = 8023;

    static final Addressable SELF_ADDRESS = new Addressable("localhost");

    public static final String KAFKA_TOPIC = "transactions";
    public static final int KAFKA_CONSUME_DELAY_MS = 100;
    public static final int KAFKA_DEFAULT_PARTITION = 0;
    public static final int KAFKA_INIT_SLEEP = 1000;

    public static final String PSQL_ADDRESS = "jdbc:postgresql://localhost/dissmessageapp";
    public static final String[] PSQL_USER_PASS = {"postgres", "default"};
    public static final Integer PSQL_LISTEN_PORT = 8002;
    public static final Integer PSQL_LISTEN_PORT_ALT = 8022;
    public static final int PSQL_MAX_READERS = 50;

    public static final Integer LUCENE_LISTEN_PORT = 8001;
    public static final String LUCENE_DEFAULT_INDEX_DEST = "./luceneindex/index_output";
    public static final String LUCENE_TEST_INDEX_DEST  = "./luceneindex/text_index_output";
    public static final int LUCENE_MAX_READERS = 50;

    public static final int STORAGEAPI_PORT = 8000;
    public static final int STORAGEAPI_PORT_ALT = 8020;

    public static final Addressable NO_RESPONSE = new Addressable("dontrespond", 0L);

    public static final long STORAGE_SYSTEMS_POLL_TIMEOUT = 30;
    public static final TimeUnit STORAGE_SYSTEMS_POLL_UNIT = TimeUnit.SECONDS;

    public static final int NUM_STORAGE_SYSTEMS = 2;

    public static final String TEST_LUCENE_REQUEST_ADDRESS = String.format("http://localhost:%d/lucene/query",
        LUCENE_LISTEN_PORT);
    public static final String TEST_PSQL_REQUEST_ADDRESS = String.format("http://localhost:%d/psql/query",
        PSQL_LISTEN_PORT);
    public static final String TEST_VAVR_REQUEST_ADDRESS = String.format("http://localhost:%d/vavr/query",
        VAVR_LISTEN_PORT);

    public static final String TEST_KAFKA_ADDRESS = "localhost:9092"; //"192.168.1.51:9092";
    public static final String TEST_STORAGEAPI_ADDRESS = String.format("http://localhost:%d/server", STORAGEAPI_PORT);
    public static final String TEST_STORAGEAPI_ADDRESS_ALT = String.format("http://localhost:%d/server",
        STORAGEAPI_PORT_ALT);
    public static final String TEST_LUCENE_PSQL_CONTACT_ENDPOINT = "http://localhost:" + PSQL_LISTEN_PORT +
        "/psql/contact";
    public static final String TEST_LUCENE_PSQL_CONTACT_ENDPOINT_ALT = "http://localhost:" + PSQL_LISTEN_PORT_ALT +
        "/psql/contact";

//    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    public static final int TEST_STORAGEAPI_MAX_OUTSTANDING_FAVOURS = 1000;

    static final String DEFAULT_USER = "unknown test user";

    public static final Gson gson = new Gson();
}
