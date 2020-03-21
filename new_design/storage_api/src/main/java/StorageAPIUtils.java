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

        public List<Tuple2<StupidStreamObject.ObjectType, String>> httpFavoursList = List.of(/*
            new Tuple2<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, Constants.TEST_PSQL_REQUEST_ADDRESS),
            new Tuple2<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, Constants.TEST_PSQL_REQUEST_ADDRESS),
            new Tuple2<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, Constants.TEST_LUCENE_REQUEST_ADDRESS)*/);

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
            new RequestNOP().toStupidStreamObject(new Addressable(Constants.TEST_STORAGEAPI_ADDRESS, 0L)));
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(initArgs.listeningPort));

        StorageAPI ret = new StorageAPI(producer, httpStorageSystem, initArgs.transactionsTopic, "localhost",
            initArgs.httpFavoursList, Constants.TEST_STORAGEAPI_MAX_OUTSTANDING_FAVOURS);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }

    static StorageAPI initFromArgsWithDummyKafkaForTests(StorageAPIInitArgs initArgs) throws IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer = new DummyProducer();
        KafkaUtils.produceMessage(producer, initArgs.transactionsTopic,
            new RequestNOP().toStupidStreamObject(new Addressable(Constants.TEST_STORAGEAPI_ADDRESS, 0L)));
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + initArgs.listeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(initArgs.listeningPort));

        StorageAPI ret =  new StorageAPI(producer, httpStorageSystem, initArgs.transactionsTopic, "localhost",
            initArgs.httpFavoursList, Constants.TEST_STORAGEAPI_MAX_OUTSTANDING_FAVOURS);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }
}
