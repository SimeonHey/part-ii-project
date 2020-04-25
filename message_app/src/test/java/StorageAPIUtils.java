import io.vavr.Tuple2;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

class StorageAPIUtils {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIUtils.class.getName());

    static class StorageAPIInitArgs {
        public String kafkaAddress;
        public String transactionsTopic;
        public int listeningPort;

        public List<Tuple2<String, List<String>>> httpFavoursList = List.of(/*
            new Tuple2<>(RequestAllMessages.class.getName(),
                List.of(ConstantsMAPP.TEST_PSQL_REQUEST_ADDRESS, ConstantsMAPP.TEST_VAVR_REQUEST_ADDRESS)),
            new Tuple2<>(RequestMessageDetails.class.getName(),
                List.of(ConstantsMAPP.TEST_PSQL_REQUEST_ADDRESS)),
            new Tuple2<>(RequestSearchMessage.class.getName(),
                List.of(ConstantsMAPP.TEST_LUCENE_REQUEST_ADDRESS)),
            new Tuple2<>(RequestGetUnreadMessages.class.getName(),
                List.of(ConstantsMAPP.TEST_VAVR_REQUEST_ADDRESS))*/);

        private StorageAPIInitArgs () {

        }

        public static StorageAPIInitArgs customValues(String argKafkaAddress, String argTransactionsTopic,
                                          int argListeningPort) {
            StorageAPIInitArgs ret = new StorageAPIInitArgs();

            ret.kafkaAddress = argKafkaAddress;
            ret.transactionsTopic = argTransactionsTopic;
            ret.listeningPort = argListeningPort;

            return ret;
        }

        public static StorageAPIInitArgs defaultTestValues() {
            return customValues(ConstantsMAPP.TEST_KAFKA_ADDRESS, ConstantsMAPP.KAFKA_TOPIC, ConstantsMAPP.STORAGEAPI_PORT);
        }
    }

    static PolyglotAPI initFromArgsForTests(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, EventBase> producer =
            KafkaUtils.createProducer(initArgs.kafkaAddress, "storageAPI");
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(initArgs.listeningPort));

        PolyglotAPI ret = new PolyglotAPI(producer, httpStorageSystem, initArgs.transactionsTopic, "localhost",
            initArgs.httpFavoursList);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }

    static PolyglotAPI initFromArgsWithDummyKafkaForTests(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, EventBase> producer = new DummyProducer();
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(initArgs.listeningPort));

        PolyglotAPI ret =  new PolyglotAPI(producer, httpStorageSystem, initArgs.transactionsTopic, "localhost",
            initArgs.httpFavoursList);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }
}
