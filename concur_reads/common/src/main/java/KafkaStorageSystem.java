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

            if (streamObject.getObjectType() == StupidStreamObject.ObjectType.NOP) {
                LOGGER.info("Received a NOP. Skipping...");

                // Write requests
            } else if (streamObject.getObjectType() == StupidStreamObject.ObjectType.POST_MESSAGE) {
                this.postMessage(RequestPostMessage.fromStupidStreamObject(streamObject, requestUUID));
            } else if (streamObject.getObjectType() == StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES) {
                this.deleteAllMessages();
            }

            // Read requests
            else if (streamObject.getObjectType() == StupidStreamObject.ObjectType.SEARCH_MESSAGES) {
                executeReadOperation((snapshotHolder) -> this.searchMessage(snapshotHolder,
                    RequestSearchMessage.fromStupidStreamObject(streamObject, requestUUID)));

            } else if (streamObject.getObjectType() == StupidStreamObject.ObjectType.GET_ALL_MESSAGES) {
                executeReadOperation((snapshotHolder) -> this.getAllMessages(snapshotHolder,
                    RequestAllMessages.fromStupidStreamObject(streamObject, requestUUID)));

            } else if (streamObject.getObjectType() == StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS) {
                executeReadOperation((snapshotHolder) -> this.getMessageDetails(snapshotHolder,
                    RequestMessageDetails.fromStupidStreamObject(streamObject, requestUUID)));

            } else if (streamObject.getObjectType() == StupidStreamObject.ObjectType.SEARCH_AND_DETAILS) {
                executeReadOperation((snapshotHolder) -> this.searchAndDetails(snapshotHolder,
                    RequestSearchAndDetails.fromStupidStreamObject(streamObject, requestUUID)));

            } else {
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
        SnapshotHolder<T> snapshotHolder = getSnapshot();

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

    public abstract SnapshotHolder<T> getSnapshot();

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
