import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.OutputStream;

public class LuceneStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject> {
    private LuceneWrapper luceneWrapper;
    private Gson gson;

    public LuceneStorageSystem(SubscribableConsumer<Long, StupidStreamObject> consumer,
                               HttpServer httpServer,
                               LuceneWrapper luceneWrapper,
                               Gson gson) {
        this.luceneWrapper = luceneWrapper;
        this.gson = gson;

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

        StupidStreamObject streamObject = message.value();
        switch (streamObject.getObjectType()) {
            case POST_MESSAGE:
                this.luceneWrapper.postMessage(new PostMessageRequest(streamObject));
                break;
            case SEARCH_MESSAGES:
                // this.luceneWrapper.searchMessage(new SearchMessageRequest(streamObject));
                System.out.println("Warning: Search via log is not implemented");
                break;
            default:
                throw new RuntimeException("Unknown stream object type");
        }
    }

    private void handleSearch(HttpExchange httpExchange) throws IOException {
        String query = httpExchange.getRequestURI().getQuery();

        SearchMessageResponse searchResult = this.luceneWrapper.searchMessage(new SearchMessageRequest(query));
        String serialized = gson.toJson(searchResult);

        httpExchange.sendResponseHeaders(200, serialized.getBytes().length);

        OutputStream os = httpExchange.getResponseBody();
        os.write(serialized.getBytes());
        os.close();

        httpExchange.close();
    }

    private void handleDiscover(HttpExchange httpExchange) throws IOException {
        httpExchange.sendResponseHeaders(200, 0);
        httpExchange.close();
    }
}
