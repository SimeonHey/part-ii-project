public class Constants {
    public static final String KAFKA_ADDRESS = "localhost:9092";
    public static final String KAFKA_TOPIC = "transactions";
    public static final int KAFKA_CONSUME_DELAY_MS = 100;

    public static final String PSQL_ADDRESS = "jdbc:postgresql://localhost/simple_olep";
    public static final String[] PSQL_USER_PASS = {"postgres", "default"};

    public static final String STORAGEAPI_PORT = "8000";
    public static final String STORAGEAPI_ADDRESS = String.format("http://localhost:%s/server", STORAGEAPI_PORT);
}
