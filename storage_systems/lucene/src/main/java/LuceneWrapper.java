import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.OutputStream;

public class LuceneWrapper implements KafkaConsumerObserver<Long, StupidStreamObject> {
    public LuceneWrapper(SubscribableConsumer<Long, StupidStreamObject> consumer, HttpServer httpServer) {
        consumer.subscribe(this);
        httpServer.createContext("/lucene/search", this::handleSearch);
        httpServer.createContext("/lucene/discover", this::handleDiscover);
    }

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        System.out.println("Lucene received values of type " + message.value().getObjectType().toString() + " with " +
            "properties:");
        message.value().getProperties().forEach((key, value) ->
            System.out.println(key + " - " + value));
    }

    private void handleSearch(HttpExchange httpExchange) throws IOException {
        String query = "URL params were: " + httpExchange.getRequestURI().getQuery();

        httpExchange.sendResponseHeaders(200, query.getBytes().length);

        OutputStream os = httpExchange.getResponseBody();
        os.write(query.getBytes());
        os.close();

        httpExchange.close();
    }

    private void handleDiscover(HttpExchange httpExchange) throws IOException {
        httpExchange.sendResponseHeaders(200, 0);
        httpExchange.close();
    }
}
