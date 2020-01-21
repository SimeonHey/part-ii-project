public class Constants {
    public static final String KAFKA_ADDRESS = "localhost:9092";
    public static final String KAFKA_TOPIC = "transactions";
    public static final int KAFKA_CONSUME_DELAY_MS = 100;
    public static final int KAFKA_DEFAULT_PARTITION = 0;
    public static final int KAFKA_INIT_SLEEP = 1000;

    public static final String PSQL_ADDRESS = "jdbc:postgresql://localhost/simple_olep";
    public static final String[] PSQL_USER_PASS = {"postgres", "default"};
    public static final String PSQL_LISTEN_PORT = "8002";
    public static final String PSQL_LISTEN_PORT_ALT = "8022";

    public static final String LUCENE_PSQL_CONTACT_ENDPOINT = "http://localhost:" + PSQL_LISTEN_PORT +
        "/psql/luceneContact";
    public static final String LUCENE_PSQL_CONTACT_ENDPOINT_ALT = "http://localhost:" + PSQL_LISTEN_PORT_ALT +
        "/psql/luceneContact";

    public static final String STORAGEAPI_PORT = "8000";
    public static final String STORAGEAPI_PORT_ALT = "8020";
    public static final String STORAGEAPI_ADDRESS = String.format("http://localhost:%s/server", STORAGEAPI_PORT);
    public static final String STORAGEAPI_ADDRESS_ALT = String.format("http://localhost:%s/server",
        STORAGEAPI_PORT_ALT);

    public static final String random = "Hello from sd_req_sesh";
}
