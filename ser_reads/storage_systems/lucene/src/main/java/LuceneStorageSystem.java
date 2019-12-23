import com.google.gson.Gson;

import java.util.List;
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
    public void searchMessage(RequestSearchMessage requestSearchMessage, long uuid) {
        LOGGER.info(String.format("Handling search request through log %s", requestSearchMessage));

        List<Long> occurrences =
            this.luceneWrapper.searchMessage(requestSearchMessage.getSearchText());
        ResponseSearchMessage searchResult = new ResponseSearchMessage(occurrences);
        MultithreadedResponse fullResponse = new MultithreadedResponse(uuid, searchResult);

        String serialized = gson.toJson(fullResponse);

        LOGGER.info(String.format("Serialized results of the search are: %s", serialized));

        this.sendResponse(serverAddress, requestSearchMessage.getResponseEndpoint(), serialized);
    }

    @Override
    public void postMessage(RequestPostMessage postMessage, long uuid) {
        this.luceneWrapper.postMessage(postMessage.getMessage(), uuid);
    }

    @Override
    public void deleteAllMessages() {
        this.luceneWrapper.deleteAllMessages();
    }
}
