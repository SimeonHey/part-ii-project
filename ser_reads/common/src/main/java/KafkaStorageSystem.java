import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.logging.Logger;

public abstract class KafkaStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject> {
    private final static Logger LOGGER = Logger.getLogger(KafkaStorageSystem.class.getName());

    public KafkaStorageSystem(SubscribableConsumer<Long, StupidStreamObject> consumer) {
        consumer.subscribe(this);
    }

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        LOGGER.info("Lucene received values of type " + message.value().getObjectType().toString() + " with " +
            "properties:");
        message.value().getProperties().forEach((key, value) ->
            LOGGER.info(key + " - " + value));

        StupidStreamObject streamObject = message.value();
        long uuid = message.offset();
        switch (streamObject.getObjectType()) {
            case POST_MESSAGE:
                this.postMessage(new RequestPostMessage(streamObject), uuid);
                break;
            case DELETE_ALL_MESSAGES:
                this.deleteAllMessages();
            case NOP:
                LOGGER.info("Received a NOP. Skipping...");
                break;
            case SEARCH_MESSAGES:
                this.searchMessage(new RequestSearchMessage(streamObject));
                break;
            default:
                LOGGER.warning("Received unkown message type");
                throw new RuntimeException("Unknown stream object type");
        }
    }

    public void sendResponse(String base, String endpoint, String resp) {
        try {
            HttpUtils.sendHttpGetRequest(base, endpoint, resp);
        } catch (IOException e) {
            LOGGER.warning("Failed to send a response back");
            throw new RuntimeException(e);
        }
    }

    public abstract void searchMessage(RequestSearchMessage requestSearchMessage);
    public abstract void postMessage(RequestPostMessage postMessage, long uuid);
    public abstract void deleteAllMessages();
}
