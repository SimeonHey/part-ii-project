import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class StorageAPI implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(StorageAPI.class.getName());
    private static final String ENDPOINT_RESPONSE = "response";
    private static final String ENDPOINT_CONFIRMATION = "confirmation";

    private final String ADDRESS_CONFIRMATION;
    private final String ADDRESS_RESPONSE;

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
        this.httpStorageSystem.registerHandler(ENDPOINT_CONFIRMATION, this::receiveConfirmation);

        this.multithreadedCommunication = new MultithreadedCommunication();

        this.ADDRESS_CONFIRMATION =  String.format("http://localhost:%s/%s/%s",
            httpStorageSystem.getPort(), httpStorageSystem.getStorageSystemName(), ENDPOINT_CONFIRMATION);
        this.ADDRESS_RESPONSE =  String.format("http://localhost:%s/%s/%s",
            httpStorageSystem.getPort(), httpStorageSystem.getStorageSystemName(), ENDPOINT_RESPONSE);

        LOGGER.info("Address confirmation: " + ADDRESS_CONFIRMATION);
        LOGGER.info("Address response: " + ADDRESS_RESPONSE);
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
        // Post message in this thread
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            request);

        // Return a future which waits for the response
        return CompletableFuture.supplyAsync(() -> {
            try {
                return this.multithreadedCommunication.consumeAndDestroy(offset);
            } catch (InterruptedException e) {
                LOGGER.warning("Error when waiting to receive response in a new thread");
                throw new RuntimeException(e);
            }
        });
    }

    private byte[] receiveResponse(String serializedResponse) {
        LOGGER.info(String.format("Received response %s", serializedResponse));

        this.multithreadedCommunication.registerResponse(serializedResponse);
        return ("Received response " + serializedResponse).getBytes();
    }

    private byte[] receiveConfirmation(String serializedResponse) {
        LOGGER.info(String.format("Received confirmation: %s", serializedResponse));
        return ("Thanks!").getBytes();
    }

    // Reads
    public CompletableFuture<ResponseMessageDetails> searchAndDetailsFuture(String searchText) {
        return kafkaRequestResponseFuture(RequestSearchAndDetails.getStupidStreamObject(searchText, ADDRESS_RESPONSE))
            .thenApply(serializedResponse -> gson.fromJson(serializedResponse, ResponseMessageDetails.class));
    }

    public ResponseMessageDetails searchAndDetails(String searchText) throws InterruptedException {
        String serializedResponse = kafkaRequestResponse(
            RequestSearchAndDetails.getStupidStreamObject(searchText, ADDRESS_RESPONSE));
        return gson.fromJson(serializedResponse, ResponseMessageDetails.class);
    }

    public ResponseSearchMessage searchMessage(String searchText) throws InterruptedException {
        String serializedResponse = kafkaRequestResponse(
            RequestSearchMessage.getStupidStreamObject(searchText, ADDRESS_RESPONSE));
        return gson.fromJson(serializedResponse, ResponseSearchMessage.class);
    }

    public ResponseMessageDetails messageDetails(Long uuid) throws InterruptedException {
        String serializedResponse = kafkaRequestResponse(
            RequestMessageDetails.getStupidStreamObject(uuid, ADDRESS_RESPONSE)
        );
        return gson.fromJson(serializedResponse, ResponseMessageDetails.class);
    }

    public ResponseAllMessages allMessages() throws InterruptedException {
        String serializedResponse = kafkaRequestResponse(
            RequestAllMessages.getStupidStreamObject(ADDRESS_RESPONSE)
        );
        return gson.fromJson(serializedResponse, ResponseAllMessages.class);
    }

    // Writes
    public void postMessage(Message message) {
        LOGGER.info("Posting message " + message);
        KafkaUtils.produceMessage(this.producer,
            this.transactionsTopic,
            new RequestPostMessage(message, -1).toStupidStreamObject(ADDRESS_CONFIRMATION)
        );
    }

    public void deleteAllMessages() {
        KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            new StupidStreamObject(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, ADDRESS_CONFIRMATION)
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
