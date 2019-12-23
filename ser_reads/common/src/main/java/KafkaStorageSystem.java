import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.logging.Logger;

public abstract class KafkaStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject> {
    private final static Logger LOGGER = Logger.getLogger(KafkaStorageSystem.class.getName());
    private final Gson gson = new Gson();

    private final String serverAddress;

    public KafkaStorageSystem(SubscribableConsumer<Long, StupidStreamObject> consumer,
                              String serverAddress) {
        this.serverAddress = serverAddress;

        consumer.subscribe(this);
    }

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        LOGGER.info("Lucene received values of type " + message.value().getObjectType().toString() + " with " +
            "properties:");
        message.value().getProperties().forEach((key, value) ->
            LOGGER.info(key + " - " + value));

        StupidStreamObject streamObject = message.value();
        long requestUUID = message.offset();

        switch (streamObject.getObjectType()) {
            case POST_MESSAGE:
                this.postMessage(RequestPostMessage.fromStupidStreamObject(streamObject, requestUUID));
                break;
            case DELETE_ALL_MESSAGES:
                this.deleteAllMessages();
            case NOP:
                LOGGER.info("Received a NOP. Skipping...");
                break;
            case SEARCH_MESSAGES:
                this.searchMessage(RequestSearchMessage.fromStupidStreamObject(streamObject, requestUUID));
                break;
            case GET_ALL_MESSAGES:
                this.getAllMessages(RequestAllMessages.fromStupidStreamObject(streamObject, requestUUID));
                break;
            case GET_MESSAGE_DETAILS:
                this.getMessageDetails(RequestMessageDetails.fromStupidStreamObject(streamObject, requestUUID));
                break;

            default:
                LOGGER.warning("Received unkown message type");
                throw new RuntimeException("Unknown stream object type");
        }
    }

    protected void sendResponse(RequestWithResponse request, Object resp) {
        try {
            MultithreadedResponse fullResponse = new MultithreadedResponse(request.getUuid(), resp);
            String serialized = this.gson.toJson(fullResponse);

            LOGGER.info(String.format("Sending response %s to server %s and endpoint %s", serialized,
                this.serverAddress, request.getResponseEndpoint()));

            HttpUtils.httpRequestResponse(this.serverAddress, request.getResponseEndpoint(), serialized);
        } catch (IOException e) {
            LOGGER.warning("Failed to send a response back");
            throw new RuntimeException(e);
        }
    }

    public abstract void getMessageDetails(RequestMessageDetails requestMessageDetails);
    public abstract void getAllMessages(RequestAllMessages requestAllMessages);
    public abstract void searchMessage(RequestSearchMessage requestSearchMessage);
    public abstract void postMessage(RequestPostMessage postMessage);
    public abstract void deleteAllMessages();
}
