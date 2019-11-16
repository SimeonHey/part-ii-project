import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class EntryPoint {
    public static void main(String[] args) throws IOException {
        // Consume program line arguments
        int argListeningPort = Integer.parseInt(args[0]);

        // Connect to Kafka & possibly the storage api
        SubscribableConsumer<Long, StupidStreamObject> consumer =
            new SubscribableConsumer<>(KafkaUtils.createConsumer());
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(argListeningPort), 0);
        LuceneWrapper luceneWrapper = new LuceneWrapper(consumer, httpServer);

        httpServer.setExecutor(null); // creates a default executor
        httpServer.start();
        // Consume from Kafka topic & do things
        consumer.listenBlockingly();

        // Send results back to the storage api
    }
}
