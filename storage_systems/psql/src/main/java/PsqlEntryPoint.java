import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class PsqlEntryPoint {
    public static void main(String[] args) throws IOException {
        // Consume program line arguments
        int argListeningPort = Integer.parseInt(args[0]);

        // Connect to Kafka & possibly the storage api
        LoopingConsumer<Long, StupidStreamObject> consumer =
            new LoopingConsumer<>(KafkaUtils.createConsumer());
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(argListeningPort), 0);
        PsqlWrapper psqlWrapper = new PsqlWrapper();
        Gson gson = new Gson();

        PsqlStorageSystem luceneStorageSystem = new PsqlStorageSystem(consumer, httpServer, psqlWrapper, gson);

        // Listen for requests & consume from Kafka topic
        httpServer.setExecutor(null); // creates a default executor
        httpServer.start();

        consumer.listenBlockingly();

        // Send results back to the storage api
    }
}
