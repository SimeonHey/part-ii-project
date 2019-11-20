import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class LuceneEntryPoint {
    public static void main(String[] args) throws IOException {
        // Consume program line arguments
        int argListeningPort = Integer.parseInt(args[0]);

        // Connect to Kafka & possibly the storage api
        LoopingConsumer<Long, StupidStreamObject> consumer =
            new LoopingConsumer<>(KafkaUtils.createConsumer("lucene"));
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(argListeningPort), 0);
        LuceneWrapper luceneWrapper = new LuceneWrapper();
        Gson gson = new Gson();

        LuceneStorageSystem luceneStorageSystem = new LuceneStorageSystem(consumer, httpServer, luceneWrapper, gson);

        // Listen for requests & consume from Kafka topic
        httpServer.setExecutor(null); // creates a default executor
        httpServer.start();

        consumer.listenBlockingly();

        // Send results back to the storage api
    }
}
