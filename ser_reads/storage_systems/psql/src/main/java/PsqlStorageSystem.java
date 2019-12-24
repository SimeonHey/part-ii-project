import java.util.logging.Logger;

public class PsqlStorageSystem extends KafkaStorageSystem {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystem.class.getName());
    
    private final PsqlWrapper psqlWrapper;

    PsqlStorageSystem(PsqlWrapper psqlWrapper,
                      String serverAddress) {
        super(serverAddress);

        this.psqlWrapper = psqlWrapper;
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
