import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.logging.Logger;

class StorageAPIUtils {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIUtils.class.getName());

    static class StorageAPIInitArgs {
        public String kafkaAddress;
        public String transactionsTopic;
        public String listeningPort;

        private StorageAPIInitArgs () {

        }

        public static StorageAPIInitArgs customValues(String argKafkaAddress, String argTransactionsTopic,
                                          String argListeningPort) {
            StorageAPIInitArgs ret = new StorageAPIInitArgs();

            ret.kafkaAddress = argKafkaAddress;
            ret.transactionsTopic = argTransactionsTopic;
            ret.listeningPort = argListeningPort;

            return ret;
        }

        public static StorageAPIInitArgs defaultValues() {
            return customValues(Constants.KAFKA_ADDRESS, Constants.KAFKA_TOPIC, Constants.STORAGEAPI_PORT);
        }
    }

    static StorageAPI initFromArgs(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer =
            KafkaUtils.createProducer(initArgs.kafkaAddress, "storageAPI");
        KafkaUtils.produceMessage(producer, initArgs.transactionsTopic,
            RequestNOP.toStupidStreamObject(Constants.STORAGEAPI_ADDRESS));
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(Integer.parseInt(initArgs.listeningPort)));

        StorageAPI ret =
            new StorageAPI(new Gson(), producer, httpStorageSystem, initArgs.transactionsTopic);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }

    static StorageAPI initFromArgsWithDummyKafka(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer = new DummyProducer();
        KafkaUtils.produceMessage(producer, initArgs.transactionsTopic,
            RequestNOP.toStupidStreamObject(Constants.STORAGEAPI_ADDRESS));
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(Integer.parseInt(initArgs.listeningPort)));

        StorageAPI ret =
            new StorageAPI(new Gson(), producer, httpStorageSystem, initArgs.transactionsTopic);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }
}
