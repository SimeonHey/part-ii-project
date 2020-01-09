import com.google.gson.Gson;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class LuceneStorageSystem extends KafkaStorageSystem implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystem.class.getName());
    
    private final LuceneWrapper luceneWrapper;
    private final String psqlContactAddress;

    private final Gson gson = new Gson();

    LuceneStorageSystem(LuceneWrapper luceneWrapper,
                        String serverAddress,
                        String psqlContactAddress) {
        super(serverAddress);
        this.luceneWrapper = luceneWrapper;
        this.psqlContactAddress = psqlContactAddress;
    }

    @Override
    public void searchAndDetails(RequestSearchAndDetails requestSearchAndDetails) {
        LOGGER.info(String.format("Handling search and details request through log %s", requestSearchAndDetails));

        List<Long> occurrences =
            this.luceneWrapper.searchMessage(requestSearchAndDetails.getSearchText());

        if (occurrences.size() == 0) {
            LOGGER.info("No occurrences were found; sending an empty details response");

            this.sendResponse(requestSearchAndDetails,
                new ResponseMessageDetails(null, -1));
            return;
        }

        LOGGER.info("Got occurrences " + occurrences);

        RequestMessageDetails requestMessageDetails = new RequestMessageDetails(occurrences.get(0),
            requestSearchAndDetails.responseEndpoint, requestSearchAndDetails.getUuid());
        MultithreadedResponse fullResponse = new MultithreadedResponse(requestSearchAndDetails.getUuid(),
            requestMessageDetails);

        try {
            // PSQL will send the response to the requester
            HttpUtils.httpRequestResponse(this.psqlContactAddress, gson.toJson(fullResponse));
        } catch (IOException e) {
            LOGGER.warning("Error when conctacting PSQL with the search results");
            throw new RuntimeException(e);
        }
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
        LOGGER.info("Lucene received a post message " + postMessage);
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
