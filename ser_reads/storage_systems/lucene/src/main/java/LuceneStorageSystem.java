import java.util.List;
import java.util.logging.Logger;

public class LuceneStorageSystem extends KafkaStorageSystem implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystem.class.getName());
    
    private final LuceneWrapper luceneWrapper;

    LuceneStorageSystem(LuceneWrapper luceneWrapper,
                        String serverAddress) {
        super(serverAddress);
        this.luceneWrapper = luceneWrapper;
    }

    @Override
    public void getMessageDetails(RequestMessageDetails requestMessageDetails) {
        LOGGER.info("Lucene received a get message details request and ignores it");
    }

    @Override
    public void getAllMessages(RequestAllMessages requestAllMessages) {
        LOGGER.info("Lucene received a get all messages request and ignores it");
    }

    @Override
    public void searchMessage(RequestSearchMessage requestSearchMessage) {
        LOGGER.info(String.format("Handling search request through log %s", requestSearchMessage));

        List<Long> occurrences =
            this.luceneWrapper.searchMessage(requestSearchMessage.getSearchText());
        LOGGER.info("Got occurrences " + occurrences);

        ResponseSearchMessage searchResult = new ResponseSearchMessage(occurrences);
        this.sendResponse(requestSearchMessage, searchResult);
    }

    @Override
    public void postMessage(RequestPostMessage postMessage) {
        this.luceneWrapper.postMessage(postMessage.getMessage(), postMessage.getUuid());
    }

    @Override
    public void deleteAllMessages() {
        this.luceneWrapper.deleteAllMessages();
    }

    @Override
    public void close() throws Exception {
        luceneWrapper.close();
    }
}
