import com.google.gson.Gson;

import java.util.concurrent.TimeUnit;

public class Constants {
    public static final String KAFKA_ADDRESS = "localhost:9092";
    public static final String KAFKA_TOPIC = "transactions";
    public static final int KAFKA_CONSUME_DELAY_MS = 100;
    public static final int KAFKA_DEFAULT_PARTITION = 0;
    public static final int KAFKA_INIT_SLEEP = 1000;

    public static final String PSQL_ADDRESS = "jdbc:postgresql://localhost/simple_olep";
    public static final String[] PSQL_USER_PASS = {"postgres", "default"};
    public static final Integer PSQL_LISTEN_PORT = 8002;
    public static final Integer PSQL_LISTEN_PORT_ALT = 8022;
    public static final String PSQL_REQUEST_ADDRESS = String.format("http://localhost:%d/psql/query", PSQL_LISTEN_PORT);
    public static final int PSQL_MAX_READERS = 50;

    public static final Integer LUCENE_LISTEN_PORT = 8001;
    public static final Integer LUCENE_LISTEN_PORT_ALT = 8021;
    public static final String LUCENE_REQUEST_ADDRESS = String.format("http://localhost:%d/lucene/query",
        LUCENE_LISTEN_PORT);
    public static final String LUCENE_DEFAULT_INDEX_DEST = "./luceneindex/index_output";
    public static final String LUCENE_TEST_INDEX_DEST  = "./luceneindex/text_index_output";
    public static final String LUCENE_PSQL_CONTACT_ENDPOINT = "http://localhost:" + PSQL_LISTEN_PORT +
        "/psql/contact";
    public static final String LUCENE_PSQL_CONTACT_ENDPOINT_ALT = "http://localhost:" + PSQL_LISTEN_PORT_ALT +
        "/psql/contact";
    public static final int LUCENE_MAX_READERS = 50;

    public static final int STORAGEAPI_PORT = 8000;
    public static final int STORAGEAPI_PORT_ALT = 8020;
    public static final String STORAGEAPI_ADDRESS = String.format("http://localhost:%d/server", STORAGEAPI_PORT);
    public static final String STORAGEAPI_ADDRESS_ALT = String.format("http://localhost:%d/server",
        STORAGEAPI_PORT_ALT);

    public static final String PROJECT_NAME = "new_design";
    public static final Addressable NO_RESPONSE = new Addressable("dontrespond", 0L);

    public static final long STORAGE_SYSTEMS_POLL_TIMEOUT = 30;
    public static final TimeUnit STORAGE_SYSTEMS_POLL_UNIT = TimeUnit.SECONDS;

    // TODO: Might not be the best place?
    public static final Gson gson = new Gson();

    public static final int NUM_STORAGE_SYSTEMS = 2;
}
