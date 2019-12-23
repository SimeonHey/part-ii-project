import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

class StorageAPIUtils {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIUtils.class.getName());

    static StorageAPI initFromArgs(String argPsqlAddress,
                                   String argKafkaAddress,
                                   String argTransactionsTopic,
                                   int argListeningPort) throws InterruptedException, IOException {
        LOGGER.info("Initializing storage API......");

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer =
            KafkaUtils.createProducer(argKafkaAddress, "storageAPI");
        KafkaUtils.produceMessage(producer, argTransactionsTopic, RequestNOP.toStupidStreamObject());
        LOGGER.info("Success");

        LOGGER.info("Connecting to PSQL...");
        HttpUtils.discoverEndpoint(argPsqlAddress);
        LOGGER.info("Success");

        LOGGER.info("Initializing an HTTP server on port " + argListeningPort);
        HttpStorageSystem httpStorageSystem = new HttpStorageSystem("server",
            HttpUtils.initHttpServer(argListeningPort));

        StorageAPI ret =
            new StorageAPI(new Gson(), producer, httpStorageSystem, argPsqlAddress, argTransactionsTopic);

        LOGGER.info("Successfully initialized storage api " + ret);
        return ret;
    }
}
