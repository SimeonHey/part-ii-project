import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.logging.Logger;

class StorageAPIUtils {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIUtils.class.getName());

    static class StorageAPIInitArgs {
        public String kafkaAddress;
        public String transactionsTopic;
        public int listeningPort;

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
            return customValues(Constants.TEST_KAFKA_ADDRESS, Constants.KAFKA_TOPIC, Constants.STORAGEAPI_PORT);
        }
    }

    static StorageAPI initFromArgsForTests(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer =
            KafkaUtils.createProducer(initArgs.kafkaAddress, "storageAPI");
        KafkaUtils.produceMessage(producer, initArgs.transactionsTopic,
            RequestNOP.toStupidStreamObject(new Addressable(Constants.TEST_STORAGEAPI_ADDRESS, 0L)));
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(initArgs.listeningPort));

        StorageAPI ret =
            new StorageAPI(producer, httpStorageSystem, initArgs.transactionsTopic, "localhost");

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }

    static StorageAPI initFromArgsWithDummyKafkaForTests(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer = new DummyProducer();
        KafkaUtils.produceMessage(producer, initArgs.transactionsTopic,
            RequestNOP.toStupidStreamObject(new Addressable(Constants.TEST_STORAGEAPI_ADDRESS, 0L)));
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(initArgs.listeningPort));

        StorageAPI ret =
            new StorageAPI(producer, httpStorageSystem, initArgs.transactionsTopic, "localhost");

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }
}
