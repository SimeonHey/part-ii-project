import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.Consumer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class LuceneEntryPoint {
    public static void main(String[] args) throws IOException {
        // Consume program line arguments
        int argListeningPort = Integer.parseInt(args[0]);
        String argKafkaAddress = args[1];
        String argTransactionsTopic = args[2];

        // Connect to Kafka & possibly the storage api
        Consumer<Long, StupidStreamObject> kafkaConsumer = KafkaUtils.createConsumer(
            "lucene",
            argKafkaAddress,
            argTransactionsTopic);
        LoopingConsumer<Long, StupidStreamObject> loopingConsumer =
            new LoopingConsumer<>(kafkaConsumer, 100);

        HttpServer httpServer = HttpServer.create(new InetSocketAddress(argListeningPort), 0);
        LuceneWrapper luceneWrapper = new LuceneWrapper();
        Gson gson = new Gson();

        LuceneStorageSystem luceneStorageSystem =
            new LuceneStorageSystem(loopingConsumer, httpServer, luceneWrapper, gson);

        // Listen for requests & consume from Kafka topic
        httpServer.setExecutor(null); // creates a default executor
        httpServer.start();

        loopingConsumer.listenBlockingly();
    }
}
