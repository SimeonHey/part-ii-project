import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.logging.Logger;

public abstract class EventStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject>, AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(EventStorageSystem.class.getName());

    protected final String storageSystemName;
    private final String serverAddress;

    public EventStorageSystem(String storageSystemName, String serverAddress) {
        this.storageSystemName = storageSystemName;
        this.serverAddress = serverAddress;
    }

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        try {
            long requestUUID = message.offset();

            // This just concatenates all properties in a string
            StringBuilder fullProps = new StringBuilder();
            message.value().getProperties().forEach((key, value) ->
                fullProps.append("\n").append(key).append(" - ").append(value));

            LOGGER.info(String.format("%s has received values of type %s with offset %s and properties %s",
                this.storageSystemName, message.value().getObjectType().toString(), requestUUID, fullProps.toString()));

            StupidStreamObject streamObject = message.value();

            switch (streamObject.getObjectType()) {
                case NOP:
                    LOGGER.info("Received a NOP. Skipping...");
                    break;

                // Write requests
                case POST_MESSAGE:
                    this.postMessage(RequestPostMessage.fromStupidStreamObject(streamObject, requestUUID));

                    break;
                case DELETE_ALL_MESSAGES:
                    this.deleteAllMessages();
                    break;

                // Read requests
                case SEARCH_MESSAGES:
                    this.searchMessage(RequestSearchMessage.fromStupidStreamObject(streamObject, requestUUID));

                    break;
                case GET_ALL_MESSAGES:
                    this.getAllMessages(RequestAllMessages.fromStupidStreamObject(streamObject, requestUUID));

                    break;
                case GET_MESSAGE_DETAILS:
                    this.getMessageDetails(RequestMessageDetails.fromStupidStreamObject(streamObject, requestUUID));

                    break;
                case SEARCH_AND_DETAILS:
                    this.searchAndDetails(RequestSearchAndDetails.fromStupidStreamObject(streamObject, requestUUID));

                    break;
                default:
                    LOGGER.warning("Received unkown message type");
                    throw new RuntimeException("Unknown stream object type");
            }
        } catch (Exception e) {
            LOGGER.warning("Error when consuming messages: " + e);
            throw new RuntimeException(e);
        }
    }

    protected void sendResponse(RequestWithResponse request, Object resp) {
        try {
            MultithreadedResponse fullResponse = new MultithreadedResponse(request.getRequestUUID(), resp);
            String serialized = Constants.gson.toJson(fullResponse);

            LOGGER.info(String.format("Sending response %s to server %s and endpoint %s", serialized,
                this.serverAddress, request.getResponseEndpoint()));

            HttpUtils.httpRequestResponse(this.serverAddress, request.getResponseEndpoint(), serialized);
        } catch (IOException e) {
            LOGGER.warning("Failed to send a response back");
            throw new RuntimeException(e);
        }
    }

    // Read requests
    public abstract void searchAndDetails(RequestSearchAndDetails requestSearchAndDetails);
    public abstract void getMessageDetails(RequestMessageDetails requestMessageDetails);
    public abstract void getAllMessages(RequestAllMessages requestAllMessages);
    public abstract void searchMessage(RequestSearchMessage requestSearchMessage);

    // Write requests
    public abstract void postMessage(RequestPostMessage postMessage);
    public abstract void deleteAllMessages();
}
