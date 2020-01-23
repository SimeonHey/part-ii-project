import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

public abstract class KafkaStorageSystem<T extends AutoCloseable>
    implements KafkaConsumerObserver<Long, StupidStreamObject> {

    private final static Logger LOGGER = Logger.getLogger(KafkaStorageSystem.class.getName());
    private final Gson gson = new Gson();

    private final String serverAddress;

    private final ExecutorService readersExecutorService;

    public KafkaStorageSystem(String serverAddress) {
        this.serverAddress = serverAddress;
        // TODO: Consider using multiple threads
        this.readersExecutorService = Executors.newFixedThreadPool(1);
    }

    public KafkaStorageSystem(String serverAddress, int numberOfReaderThreads) {
        this.serverAddress = serverAddress;
        this.readersExecutorService = Executors.newFixedThreadPool(numberOfReaderThreads);
    }

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        try {
            long requestUUID = message.offset();

            LOGGER.info("A storage system has received values of type " + message.value().getObjectType().toString() + " " +
                "with offset " + requestUUID + " and properties:");
            message.value().getProperties().forEach((key, value) ->
                LOGGER.info(key + " - " + value));

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
                    executeReadOperation((snapshotHolder) -> this.searchMessage(snapshotHolder,
                        RequestSearchMessage.fromStupidStreamObject(streamObject, requestUUID)));

                    break;
                case GET_ALL_MESSAGES:
                    executeReadOperation((snapshotHolder) -> this.getAllMessages(snapshotHolder,
                        RequestAllMessages.fromStupidStreamObject(streamObject, requestUUID)));

                    break;
                case GET_MESSAGE_DETAILS:
                    executeReadOperation((snapshotHolder) -> this.getMessageDetails(snapshotHolder,
                        RequestMessageDetails.fromStupidStreamObject(streamObject, requestUUID)));

                    break;
                case SEARCH_AND_DETAILS:
                    executeReadOperation((snapshotHolder) -> this.searchAndDetails(snapshotHolder,
                        RequestSearchAndDetails.fromStupidStreamObject(streamObject, requestUUID)));

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

    private void executeReadOperation(Consumer<SnapshotHolder<T>> operation) {
        // Get the snapshot in the current thread
        SnapshotHolder<T> snapshotHolder = getReadSnapshot();

        // Use it in the new thread
        readersExecutorService.submit(() -> {
            // Perform the operation
            operation.accept(snapshotHolder);

            // And close the snapshot 'connection'
            try {
                snapshotHolder.close();
            } catch (Exception e) {
                LOGGER.warning("Error when trying to close to snapshot holder");
                throw new RuntimeException(e);
            }
        });
    }

    // Returns an instaneous read snapshot of the data
    public abstract SnapshotHolder<T> getReadSnapshot();

    // Read requests
    public abstract void searchAndDetails(SnapshotHolder<T> snapshotHolder,
                                          RequestSearchAndDetails requestSearchAndDetails);
    public abstract void getMessageDetails(SnapshotHolder<T> snapshotHolder,
                                           RequestMessageDetails requestMessageDetails);
    public abstract void getAllMessages(SnapshotHolder<T> snapshotHolder,
                                        RequestAllMessages requestAllMessages);
    public abstract void searchMessage(SnapshotHolder<T> snapshotHolder,
                                       RequestSearchMessage requestSearchMessage);

    // Write requests
    public abstract void postMessage(RequestPostMessage postMessage);
    public abstract void deleteAllMessages();
}
