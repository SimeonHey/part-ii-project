import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

public class LuceneStorageSystem extends HttpStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject> {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystem.class.getName());
    
    private LuceneWrapper luceneWrapper;
    private Gson gson;

    LuceneStorageSystem(LoopingConsumer<Long, StupidStreamObject> consumer,
                        HttpServer httpServer,
                        LuceneWrapper luceneWrapper,
                        Gson gson) {
        super("lucene", httpServer);

        this.luceneWrapper = luceneWrapper;
        this.gson = gson;

        consumer.subscribe(this);
        httpServer.createContext("/lucene/search", this::handleSearch);
    }

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        LOGGER.info("Lucene received values of type " + message.value().getObjectType().toString() + " with " +
            "properties:");
        message.value().getProperties().forEach((key, value) ->
            LOGGER.info(key + " - " + value));

        StupidStreamObject streamObject = message.value();
        Long uuid = message.offset();
        switch (streamObject.getObjectType()) {
            case POST_MESSAGE:
                this.luceneWrapper.postMessage(new PostMessageRequest(streamObject), uuid);
                break;
            case SEARCH_MESSAGES:
                // this.luceneWrapper.searchMessage(new SearchMessageRequest(streamObject));
                LOGGER.info("Warning: Search via log is not implemented");
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
}
