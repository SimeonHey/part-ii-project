import com.google.gson.Gson;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

public class PsqlStorageSystem extends KafkaStorageSystem {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystem.class.getName());

    private final PsqlWrapper psqlWrapper;

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();

    private final Gson gson = new Gson();

    PsqlStorageSystem(PsqlWrapper psqlWrapper,
                      String serverAddress,
                      HttpStorageSystem httpStorageSystem) {
        super(serverAddress);
        this.psqlWrapper = psqlWrapper;

        httpStorageSystem.registerHandler("luceneContact", this::handleLuceneContact);
    }

    private byte[] handleLuceneContact(String query) {
        LOGGER.info("The psql http server received params " + query);
        multithreadedCommunication.registerResponse(query);
        return ("Received " + query).getBytes();
    }

    @Override
    public void searchAndDetails(RequestSearchAndDetails requestSearchAndDetails) {
        // Open a new connection which has the current snapshot of the data
        Connection snapshotIsolatedConnection = psqlWrapper.newSnapshotIsolatedConnection();

        // Spin up a new thread to wait for Lucene TODO: consider adding a limit of the outstanding waiting threads
        new Thread(() -> {
                try {
                    LOGGER.info("IN A NEW THREAD: waiting for lucene to contact us at UUID " +
                        requestSearchAndDetails.getUuid() + "...");

                    String serialized =
                        multithreadedCommunication.consumeAndDestroy(requestSearchAndDetails.getUuid());

                    LOGGER.info("Success! Serialized response received: " + serialized);

                    RequestMessageDetails requestMessageDetails =
                        gson.fromJson(serialized, RequestMessageDetails.class);

                    // Use the connection provided so that it's within this transaction
                    getMessageDetails(snapshotIsolatedConnection, requestMessageDetails);
                    snapshotIsolatedConnection.close();
                } catch (InterruptedException | SQLException e) {
                    LOGGER.warning("Error when waiting on Lucene to contact us at uuid " +
                        requestSearchAndDetails.getUuid());
                    throw new RuntimeException(e);
                }
            }).start();
    }

    @Override
    public void searchMessage(RequestSearchMessage requestSearchMessage) {
        LOGGER.info("PSQL received a search message request and ignores it");
    }

    @Override
    public void postMessage(RequestPostMessage postMessage) {
        this.psqlWrapper.postMessage(postMessage);
    }

    @Override
    public void deleteAllMessages() {
        this.psqlWrapper.deleteAllMessages();
    }

    @Override
    public void getMessageDetails(RequestMessageDetails requestMessageDetails) {
        ResponseMessageDetails reqResult = this.psqlWrapper.getMessageDetails(requestMessageDetails);
        this.sendResponse(requestMessageDetails, reqResult);
    }

    private void getMessageDetails(Connection connection, RequestMessageDetails requestMessageDetails) {
        ResponseMessageDetails reqResult = this.psqlWrapper.getMessageDetails(connection, requestMessageDetails);
        this.sendResponse(requestMessageDetails, reqResult);
    }

    @Override
    public void getAllMessages(RequestAllMessages requestAllMessages) {
        LOGGER.info("PSQL received a RequestAllMessages request: " + requestAllMessages);
        ResponseAllMessages reqResult = this.psqlWrapper.getAllMessages();
        this.sendResponse(requestAllMessages, reqResult);
    }
}
