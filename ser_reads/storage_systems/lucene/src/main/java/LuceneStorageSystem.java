import com.google.gson.Gson;

import java.util.logging.Logger;

public class LuceneStorageSystem extends KafkaStorageSystem {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystem.class.getName());
    
    private final LuceneWrapper luceneWrapper;
    private final Gson gson;
    private String serverAddress;

    LuceneStorageSystem(SubscribableConsumer<Long, StupidStreamObject> consumer,
                        LuceneWrapper luceneWrapper,
                        Gson gson,
                        String serverAddress) {
        super(consumer);

        this.luceneWrapper = luceneWrapper;
        this.gson = gson;
        this.serverAddress = serverAddress;
    }

    @Override
    public void searchMessage(RequestSearchMessage requestSearchMessage) {
        LOGGER.info(String.format("Handling search request through log %s", requestSearchMessage));

        ResponseSearchMessage searchResult = this.luceneWrapper.searchMessage(requestSearchMessage);
        String serialized = gson.toJson(searchResult);

        LOGGER.info(String.format("Serialized results of the search are: %s", serialized));

        this.sendResponse(serverAddress, requestSearchMessage.getResponseEndpoint(), serialized);
    }

    @Override
    public void postMessage(RequestPostMessage postMessage, long uuid) {
        this.luceneWrapper.postMessage(postMessage, uuid);
    }

    @Override
    public void deleteAllMessages() {
        this.luceneWrapper.deleteAllMessages();
    }
}
