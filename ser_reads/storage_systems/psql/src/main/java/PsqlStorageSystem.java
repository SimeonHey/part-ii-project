import com.google.gson.Gson;

import java.util.logging.Logger;

public class PsqlStorageSystem extends KafkaStorageSystem {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystem.class.getName());
    
    private final PsqlWrapper psqlWrapper;
    private final Gson gson;

    PsqlStorageSystem(SubscribableConsumer<Long, StupidStreamObject> consumer,
                      PsqlWrapper psqlWrapper,
                      String serverAddress,
                      Gson gson) {
        super(consumer, serverAddress);

        this.psqlWrapper = psqlWrapper;
        this.gson = gson;
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
