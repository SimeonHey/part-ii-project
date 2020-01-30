import com.google.gson.Gson;

import java.sql.Connection;
import java.util.logging.Logger;

public class PsqlConcurrentSnapshots extends AutoSnapshottedEventStorageSystem<Connection> implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(PsqlConcurrentSnapshots.class.getName());

    private final PsqlWrapper psqlWrapper;

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();
    private final HttpStorageSystem httpStorageSystem;
    private final Gson gson = new Gson();

    PsqlConcurrentSnapshots(PsqlWrapper psqlWrapper,
                            String serverAddress,
                            int numberOfReaderThreads,
                            HttpStorageSystem httpStorageSystem) {
        super("PSQL", serverAddress, numberOfReaderThreads);

        this.psqlWrapper = psqlWrapper;
        this.httpStorageSystem = httpStorageSystem;

        this.httpStorageSystem.registerHandler("luceneContact", this::handleLuceneContact);
    }

    private byte[] handleLuceneContact(String query) {
        LOGGER.info("The psql http server received params " + query);
        multithreadedCommunication.registerResponse(query);
        return ("Received " + query).getBytes();
    }

    @Override
    public SnapshotHolder<Connection> getReadSnapshot() {
        return new SnapshotHolder<>(this.psqlWrapper.newSnapshotIsolatedConnection());
    }

    // Read requests
    @Override
    public void searchAndDetails(SnapshotHolder<Connection> snapshotHolder,
                                 RequestSearchAndDetails requestSearchAndDetails) {
        // Open a new connection which has the current snapshot of the data
        try {
            LOGGER.info("Waiting for lucene to contact us at UUID " +
                requestSearchAndDetails.getRequestUUID() + "...");

            String serialized =
                multithreadedCommunication.consumeAndDestroy(requestSearchAndDetails.getRequestUUID());

            LOGGER.info("Success! Serialized response received: " + serialized);

            RequestMessageDetails requestMessageDetails =
                gson.fromJson(serialized, RequestMessageDetails.class);

            // Use the connection provided so that it's within this transaction
            getMessageDetails(snapshotHolder, requestMessageDetails);
        } catch (InterruptedException e) {
            LOGGER.warning("Error when waiting on Lucene to contact us at uuid " +
                requestSearchAndDetails.getRequestUUID());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void searchMessage(SnapshotHolder<Connection> snapshotHolder,
                              RequestSearchMessage requestSearchMessage) {
        LOGGER.info("PSQL received a search message request and ignores it");
    }

    @Override
    public void getMessageDetails(SnapshotHolder<Connection> snapshotHolder,
                                  RequestMessageDetails requestMessageDetails) {
        ResponseMessageDetails reqResult =
            this.psqlWrapper.getMessageDetails(snapshotHolder.getSnapshot(), requestMessageDetails);
        this.sendResponse(requestMessageDetails, reqResult);
    }

    @Override
    public void getAllMessages(SnapshotHolder<Connection> snapshotHolder,
                               RequestAllMessages requestAllMessages) {
        LOGGER.info("PSQL received a RequestAllMessages request: " + requestAllMessages);
        ResponseAllMessages reqResult = this.psqlWrapper.getAllMessages(snapshotHolder.getSnapshot());
        this.sendResponse(requestAllMessages, reqResult);
    }

    // Write requests
    @Override
    public void postMessage(RequestPostMessage postMessage) {
        this.psqlWrapper.postMessage(postMessage);
    }

    @Override
    public void deleteAllMessages() {
        this.psqlWrapper.deleteAllMessages();
    }

    @Override
    public void close() throws Exception {
        this.httpStorageSystem.close();
    }
}
