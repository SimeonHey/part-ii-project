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

        public List<Tuple2<String, String>> httpFavoursList = List.of(/*
            new Tuple2<>(RequestAllMessages.class.getName(), ConstantsMAPP.TEST_PSQL_REQUEST_ADDRESS),
            new Tuple2<>(RequestMessageDetails.class.getName(), ConstantsMAPP.TEST_PSQL_REQUEST_ADDRESS),
            new Tuple2<>(RequestSearchMessage.class.getName(), ConstantsMAPP.TEST_LUCENE_REQUEST_ADDRESS)*/);

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

    static StorageAPI initFromArgsForTests(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, BaseEvent> producer =
            KafkaUtils.createProducer(initArgs.kafkaAddress, "storageAPI");
        KafkaUtils.produceMessage(producer, initArgs.transactionsTopic,
            new RequestNOP(new Addressable(ConstantsMAPP.TEST_STORAGEAPI_ADDRESS)));
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(initArgs.listeningPort));

        StorageAPI ret = new StorageAPI(producer, httpStorageSystem, initArgs.transactionsTopic, "localhost",
            initArgs.httpFavoursList);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }

    static StorageAPI initFromArgsWithDummyKafkaForTests(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, BaseEvent> producer = new DummyProducer();
        KafkaUtils.produceMessage(producer, initArgs.transactionsTopic,
            new RequestNOP(new Addressable(ConstantsMAPP.TEST_STORAGEAPI_ADDRESS)));
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(initArgs.listeningPort));

        StorageAPI ret =  new StorageAPI(producer, httpStorageSystem, initArgs.transactionsTopic, "localhost",
            initArgs.httpFavoursList);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }
}
