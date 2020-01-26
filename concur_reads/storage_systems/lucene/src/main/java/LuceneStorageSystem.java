import com.google.gson.Gson;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class LuceneStorageSystem extends KafkaStorageSystem<IndexReader> implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystem.class.getName());
    
    private final LuceneWrapper luceneWrapper;
    private final String psqlContactAddress;

    private final Gson gson = new Gson();

    LuceneStorageSystem(LuceneWrapper luceneWrapper,
                        String serverAddress,
                        String psqlContactAddress,
                        int maxNumberOfReaders) {
        super(serverAddress, maxNumberOfReaders);
        this.luceneWrapper = luceneWrapper;
        this.psqlContactAddress = psqlContactAddress;
    }

    @Override
    public SnapshotHolder<IndexReader> getReadSnapshot() {
        return new SnapshotHolder<>(this.luceneWrapper.newSnapshotReader());
    }

    // Read requests handling
    @Override
    public void searchAndDetails(SnapshotHolder<IndexReader> snapshotHolder,
                                 RequestSearchAndDetails requestSearchAndDetails) {
        LOGGER.info(String.format("Handling search and details request through log %s", requestSearchAndDetails));

        List<Long> occurrences =
            this.luceneWrapper.searchMessage(snapshotHolder.getSnapshot(), requestSearchAndDetails.getSearchText());
        LOGGER.info("Got occurrences " + occurrences);

        long detailsForId;

        if (occurrences.size() == 0) {
            detailsForId = -1;
            LOGGER.info("No occurrences were found; sending a details request for id " + detailsForId);
        } else {
            detailsForId = occurrences.get(0);
            LOGGER.info("At least one occurrence was found; sending a details request for id " + detailsForId);
        }

        RequestMessageDetails requestMessageDetails = new RequestMessageDetails(detailsForId,
            requestSearchAndDetails.responseEndpoint, requestSearchAndDetails.getUuid());
        MultithreadedResponse fullResponse = new MultithreadedResponse(requestSearchAndDetails.getUuid(),
            requestMessageDetails);

        try {
            // We send a request to PSQL, and PSQL will send the response to the requester
            HttpUtils.httpRequestResponse(this.psqlContactAddress, gson.toJson(fullResponse));
        } catch (IOException e) {
            LOGGER.warning("Error when conctacting PSQL with the search results");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getMessageDetails(SnapshotHolder<IndexReader> snapshotHolder,
                                  RequestMessageDetails requestMessageDetails) {
        LOGGER.info("Lucene received a get message details request and ignores it");
    }

    @Override
    public void getAllMessages(SnapshotHolder<IndexReader> snapshotHolder,
                               RequestAllMessages requestAllMessages) {
        LOGGER.info("Lucene received a get all messages request and ignores it");
    }

    @Override
    public void searchMessage(SnapshotHolder<IndexReader> snapshotHolder,
                              RequestSearchMessage requestSearchMessage) {
        LOGGER.info(String.format("Handling search request through log %s", requestSearchMessage));

        List<Long> occurrences =
            this.luceneWrapper.searchMessage(snapshotHolder.getSnapshot(), requestSearchMessage.getSearchText());
        LOGGER.info("Got occurrences " + occurrences);

        ResponseSearchMessage searchResult = new ResponseSearchMessage(occurrences);
        this.sendResponse(requestSearchMessage, searchResult);
    }

    // Write requests handling
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
