import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

import java.util.logging.Logger;

class StorageAPIUtils {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIUtils.class.getName());

    static StorageAPI initFromArgs(String argLuceneAddress,
                                   String argPsqlAddress,
                                   String argKafkaAddress,
                                   String argTransactionsTopic) throws InterruptedException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer =
            KafkaUtils.createProducer(argKafkaAddress, "storageAPI");
        KafkaUtils.produceMessage(producer, argTransactionsTopic, RequestNOP.toStupidStreamObject());
        LOGGER.info("Success");

        LOGGER.info("Connecting to Lucene...");
        HttpUtils.discoverEndpoint(argLuceneAddress);
        LOGGER.info("Success");

        LOGGER.info("Connecting to PSQL...");
        HttpUtils.discoverEndpoint(argPsqlAddress);
        LOGGER.info("Success");

        StorageAPI ret =
            new StorageAPI(new Gson(), producer, argLuceneAddress, argPsqlAddress, argTransactionsTopic);
        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }
}
