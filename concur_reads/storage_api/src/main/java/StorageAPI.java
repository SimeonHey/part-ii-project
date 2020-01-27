import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class StorageAPI implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(StorageAPI.class.getName());
    private static final String ENDPOINT_RESPONSE = "response";

    private final Producer<Long, StupidStreamObject> producer;
    private final String transactionsTopic;
    private final Gson gson;

    private final MultithreadedCommunication multithreadedCommunication;
    private final HttpStorageSystem httpStorageSystem;

    StorageAPI(Gson gson,
               Producer<Long, StupidStreamObject> producer,
               HttpStorageSystem httpStorageSystem,
               String transactionsTopic) {
        this.gson = gson;
        this.producer = producer;
        this.transactionsTopic = transactionsTopic;
        this.httpStorageSystem = httpStorageSystem;

        this.httpStorageSystem.registerHandler(ENDPOINT_RESPONSE, this::receiveResponse);

        this.multithreadedCommunication = new MultithreadedCommunication();
    }

    public void postMessage(Message message) {
        LOGGER.info("Posting message " + message);
        KafkaUtils.produceMessage(this.producer,
            this.transactionsTopic,
            new RequestPostMessage(message, -1).toStupidStreamObject()
        );
    }

    private String kafkaRequestResponse(StupidStreamObject request) throws InterruptedException {
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            request);

        LOGGER.info("Waiting for response on channel with uuid " + offset);

        // Will block until a response is received
        return this.multithreadedCommunication.consumeAndDestroy(offset);
    }

    private CompletableFuture<String> kafkaRequestResponseFuture(StupidStreamObject request) {
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            request);

        return CompletableFuture.supplyAsync(() -> {
            try {
                return this.multithreadedCommunication.consumeAndDestroy(offset);
            } catch (InterruptedException e) {
                LOGGER.warning("Error when waiting to receive response in new thread");
                throw new RuntimeException(e);
            }
        });
    }

    private byte[] receiveResponse(String serializedResponse) {
        LOGGER.info(String.format("Received response %s", serializedResponse));

        this.multithreadedCommunication.registerResponse(serializedResponse);
        return ("Received response " + serializedResponse).getBytes();
    }

    public CompletableFuture<ResponseMessageDetails> searchAndDetailsFuture(String searchText) {
        return kafkaRequestResponseFuture(RequestSearchAndDetails.getStupidStreamObject(searchText, ENDPOINT_RESPONSE))
            .thenApply(serializedResponse -> gson.fromJson(serializedResponse, ResponseMessageDetails.class));
    }

    public ResponseMessageDetails searchAndDetails(String searchText) throws InterruptedException {
        String serializedResponse = kafkaRequestResponse(
            RequestSearchAndDetails.getStupidStreamObject(searchText, ENDPOINT_RESPONSE));
        return gson.fromJson(serializedResponse, ResponseMessageDetails.class);
    }

    public ResponseSearchMessage searchMessage(String searchText) throws InterruptedException {
        String serializedResponse = kafkaRequestResponse(
            RequestSearchMessage.getStupidStreamObject(searchText, ENDPOINT_RESPONSE));
        return gson.fromJson(serializedResponse, ResponseSearchMessage.class);
    }

    public ResponseMessageDetails messageDetails(Long uuid) throws InterruptedException {
        String serializedResponse = kafkaRequestResponse(
            RequestMessageDetails.getStupidStreamObject(uuid, ENDPOINT_RESPONSE)
        );
        return gson.fromJson(serializedResponse, ResponseMessageDetails.class);
    }

    public ResponseAllMessages allMessages() throws InterruptedException {
        String serializedResponse = kafkaRequestResponse(
            RequestAllMessages.getStupidStreamObject(ENDPOINT_RESPONSE)
        );
        return gson.fromJson(serializedResponse, ResponseAllMessages.class);
    }

    public void deleteAllMessages() {
        KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            new StupidStreamObject(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES)
        );
    }

    @Override
    public String toString() {
        return "StorageAPI{" +
            "producer=" + producer +
            ", transactionsTopic='" + transactionsTopic + '\'' +
            ", gson=" + gson +
            '}';
    }

    @Override
    public void close() throws Exception {
        httpStorageSystem.close();
    }
}
