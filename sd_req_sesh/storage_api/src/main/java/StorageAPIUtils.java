import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.logging.Logger;

class StorageAPIUtils {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIUtils.class.getName());

    static class StorageAPIInitArgs {
        public final String argKafkaAddress;
        public final String argTransactionsTopic;
        public final String argListeningPort;

        StorageAPIInitArgs(String argKafkaAddress, String argTransactionsTopic, String argListeningPort) {
            this.argKafkaAddress = argKafkaAddress;
            this.argTransactionsTopic = argTransactionsTopic;
            this.argListeningPort = argListeningPort;
        }

        StorageAPIInitArgs(String[] args) {
            this.argKafkaAddress = args[0];
            this.argTransactionsTopic = args[1];
            this.argListeningPort = args[2];
        }
    }

    static StorageAPI initFromArgs(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer =
            KafkaUtils.createProducer(initArgs.argKafkaAddress, "storageAPI");
        KafkaUtils.produceMessage(producer, initArgs.argTransactionsTopic, RequestNOP.toStupidStreamObject());
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.argListeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(Integer.parseInt(initArgs.argListeningPort)));

        StorageAPI ret =
            new StorageAPI(new Gson(), producer, httpStorageSystem, initArgs.argTransactionsTopic);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }

    static StorageAPI initFromArgsWithDummyKafka(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer = new DummyProducer();
        KafkaUtils.produceMessage(producer, initArgs.argTransactionsTopic, RequestNOP.toStupidStreamObject());
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.argListeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(Integer.parseInt(initArgs.argListeningPort)));

        StorageAPI ret =
            new StorageAPI(new Gson(), producer, httpStorageSystem, initArgs.argTransactionsTopic);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }
}
