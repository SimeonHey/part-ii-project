import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class LuceneStorageSystem extends HttpStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject> {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystem.class.getName());
    
    private final LuceneWrapper luceneWrapper;
    private final Gson gson;

    LuceneStorageSystem(SubscribableConsumer<Long, StupidStreamObject> consumer,
                        HttpServer httpServer,
                        LuceneWrapper luceneWrapper,
                        Gson gson) {
        super("lucene", httpServer);

        this.luceneWrapper = luceneWrapper;
        this.gson = gson;

        consumer.subscribe(this);

        this.registerHandler("search", this::handleSearch);
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
                this.luceneWrapper.postMessage(new RequestPostMessage(streamObject), uuid);
                break;
            case SEARCH_MESSAGES:
                // this.luceneWrapper.searchMessage(new SearchMessageRequest(streamObject));
                LOGGER.warning("Warning: Search via log is not implemented");
                break;
            case DELETE_ALL_MESSAGES:
                LOGGER.info("Lucene received a DELETE_ALL_MESSAGES request");
                this.luceneWrapper.deleteAllMessages();
            case NOP:
                LOGGER.info("Lucene received a NOP request. Skipping...");
                break;
            default:
                LOGGER.warning("Received unkown message type in Lucene");
                throw new RuntimeException("Unknown stream object type");
        }
    }

    private byte[] handleSearch(String query) {
        LOGGER.info(String.format("Handling search query %s", query));

        ResponseSearchMessage searchResult = this.luceneWrapper.searchMessage(new RequestSearchMessage(query));
        String serialized = gson.toJson(searchResult);

        LOGGER.info(String.format("Serialized results of the search are: %s", serialized));

        return serialized.getBytes();
    }
}
