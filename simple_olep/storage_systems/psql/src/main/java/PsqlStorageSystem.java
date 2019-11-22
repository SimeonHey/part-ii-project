import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class PsqlStorageSystem extends HttpStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject> {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystem.class.getName());
    
    private final PsqlWrapper psqlWrapper;
    private final Gson gson;

    PsqlStorageSystem(SubscribableConsumer<Long, StupidStreamObject> consumer,
                      HttpServer httpServer,
                      PsqlWrapper psqlWrapper,
                      Gson gson) {
        super("psql", httpServer);

        this.psqlWrapper = psqlWrapper;
        this.gson = gson;

        consumer.subscribe(this);
        this.registerHandler("messageDetails", this::handleGetMessageDetails);
        this.registerHandler("allMessages", this::handleGetAllMessages);
    }

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        LOGGER.info("Psql received values of type " + message.value().getObjectType().toString() + " with " +
            "properties:");
        message.value().getProperties().forEach((key, value) ->
            LOGGER.info(key + " - " + value));

        StupidStreamObject streamObject = message.value();
        Long uuid = message.offset();

        switch (streamObject.getObjectType()) {
            case POST_MESSAGE:
                this.psqlWrapper.postMessage(new RequestPostMessage(streamObject), uuid);
                break;
            case DELETE_ALL_MESSAGES:
                this.psqlWrapper.deleteAllMessages();
                break;
            case NOP:
                LOGGER.info("PSQL received a NOP request. Skipping...");
                break;
            default:
                LOGGER.warning("PSQL Unknown message type");
                throw new RuntimeException("Unknown stream object type");
        }
    }

    private byte[] handleGetMessageDetails(String query) {
        // TODO : Add sanitization
        RequestMessageDetails request = new RequestMessageDetails(Long.valueOf(query));

        ResponseMessageDetails reqResult = this.psqlWrapper.getMessageDetails(request);
        String serialized = gson.toJson(reqResult);

        return serialized.getBytes();
    }

    private byte[] handleGetAllMessages(String query) {
        RequestAllMessages requestAllMessages = new RequestAllMessages();
        ResponseAllMessages reqResult = this.psqlWrapper.getAllMessages(requestAllMessages);
        String serialized = gson.toJson(reqResult);

        return serialized.getBytes();
    }
}
