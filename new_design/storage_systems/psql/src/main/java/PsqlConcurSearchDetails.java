import com.google.gson.Gson;

import java.sql.Connection;
import java.util.logging.Logger;

public class PsqlConcurSearchDetails extends EventStorageSystem {
    private static final Logger LOGGER = Logger.getLogger(PsqlConcurSearchDetails.class.getName());

    private final PsqlWrapper psqlWrapper;

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();
    private final HttpStorageSystem httpStorageSystem;
    private final Gson gson = new Gson();

    PsqlConcurSearchDetails(PsqlWrapper psqlWrapper,
                            String serverAddress,
                            HttpStorageSystem httpStorageSystem) {
        super("PSQL", serverAddress);

        this.psqlWrapper = psqlWrapper;
        this.httpStorageSystem = httpStorageSystem;

        this.httpStorageSystem.registerHandler("luceneContact", this::handleLuceneContact);
    }

    private byte[] handleLuceneContact(String query) {
        LOGGER.info("The psql http server received params " + query);
        multithreadedCommunication.registerResponse(query);
        return ("Received " + query).getBytes();
    }

    // Read requests
    @Override
    public void searchAndDetails(RequestSearchAndDetails requestSearchAndDetails) {

        // Open a new connection which has the current snapshot of the data
        Connection snapshotIsolatedConnection = psqlWrapper.getConcurrentSnapshot();

        // Spin up a new thread to wait for Lucene TODO: consider adding a limit of the outstanding waiting threads
        new Thread(() -> {
            try {
                LOGGER.info("Waiting for lucene to contact us at UUID " +
                    requestSearchAndDetails.getRequestUUID() + "...");

                String serialized =
                    multithreadedCommunication.consumeAndDestroy(requestSearchAndDetails.getRequestUUID());

                LOGGER.info("Success! Serialized response received: " + serialized);

                RequestMessageDetails requestMessageDetails =
                    gson.fromJson(serialized, RequestMessageDetails.class);

                // Use the connection provided so that it's within this transaction
                getMessageDetails(snapshotIsolatedConnection, requestMessageDetails);
            } catch (InterruptedException e) {
                LOGGER.warning("Error when waiting on Lucene to contact us at uuid " +
                    requestSearchAndDetails.getRequestUUID());
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void searchMessage(RequestSearchMessage requestSearchMessage) {
        LOGGER.info("PSQL received a search message request and ignores it");
    }

    @Override
    public void getMessageDetails(RequestMessageDetails requestMessageDetails) {
        ResponseMessageDetails reqResult =
            this.psqlWrapper.getMessageDetails(requestMessageDetails);
        this.sendResponse(requestMessageDetails, reqResult);
    }

    // A bit ugly: I'm overriding the method because I need it for the search
    private void getMessageDetails(Connection connection, RequestMessageDetails requestMessageDetails) {
        ResponseMessageDetails reqResult = this.psqlWrapper.getMessageDetails(connection, requestMessageDetails);
        this.sendResponse(requestMessageDetails, reqResult);
    }

    @Override
    public void getAllMessages(RequestAllMessages requestAllMessages) {
        LOGGER.info("PSQL received a RequestAllMessages request: " + requestAllMessages);
        ResponseAllMessages reqResult = this.psqlWrapper.getAllMessages(requestAllMessages);
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
