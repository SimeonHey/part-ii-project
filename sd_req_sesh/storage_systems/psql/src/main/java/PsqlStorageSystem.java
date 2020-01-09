import com.google.gson.Gson;

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
        try {
            LOGGER.info("Waiting for lucene to contact us at UUID " + requestSearchAndDetails.getUuid() + "...");

            String serialized =
                multithreadedCommunication.consumeAndDestroy(requestSearchAndDetails.getUuid());

            LOGGER.info("Success! Serialized response received: " + serialized);

            RequestMessageDetails requestMessageDetails = gson.fromJson(serialized, RequestMessageDetails.class);
            getMessageDetails(requestMessageDetails);
        } catch (InterruptedException e) {
            LOGGER.warning("Error when waiting on Lucene to contact us at uuid " +
                requestSearchAndDetails.getUuid());
            throw new RuntimeException(e);
        }
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

    @Override
    public void getAllMessages(RequestAllMessages requestAllMessages) {
        ResponseAllMessages reqResult = this.psqlWrapper.getAllMessages();
        this.sendResponse(requestAllMessages, reqResult);
    }
}
