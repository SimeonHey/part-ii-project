import com.google.gson.Gson;

import java.util.logging.Logger;

public class PsqlStorageSystem extends KafkaStorageSystem {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystem.class.getName());
    
    private final PsqlWrapper psqlWrapper;
    private final Gson gson;

    PsqlStorageSystem(SubscribableConsumer<Long, StupidStreamObject> consumer,
                      HttpStorageSystem httpStorageSystem,
                      PsqlWrapper psqlWrapper,
                      Gson gson) {
        super(consumer);

        this.psqlWrapper = psqlWrapper;
        this.gson = gson;

        httpStorageSystem.registerHandler("messageDetails", this::httpGetMessageDetails);
        httpStorageSystem.registerHandler("allMessages", this::httpGetAllMessages);
    }

    @Override
    public void searchMessage(RequestSearchMessage requestSearchMessage, long uuid) {
        LOGGER.info("PSQL received a search message request and ignores it");
    }

    @Override
    public void postMessage(RequestPostMessage postMessage, long uuid) {
        this.psqlWrapper.postMessage(postMessage, uuid);
    }

    @Override
    public void deleteAllMessages() {
        this.psqlWrapper.deleteAllMessages();
    }

    private byte[] httpGetMessageDetails(String query) {
        // TODO : Add sanitization
        RequestMessageDetails request = new RequestMessageDetails(Long.valueOf(query));

        ResponseMessageDetails reqResult = this.psqlWrapper.getMessageDetails(request);
        String serialized = gson.toJson(reqResult);

        return serialized.getBytes();
    }

    private byte[] httpGetAllMessages(String query) {
        RequestAllMessages requestAllMessages = new RequestAllMessages();
        ResponseAllMessages reqResult = this.psqlWrapper.getAllMessages(requestAllMessages);
        String serialized = gson.toJson(reqResult);

        return serialized.getBytes();
    }
}
