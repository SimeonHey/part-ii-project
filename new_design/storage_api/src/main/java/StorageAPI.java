import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.logging.Logger;

public class StorageAPI implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(StorageAPI.class.getName());
    private static final String ENDPOINT_RESPONSE = "response";
    private static final String ENDPOINT_CONFIRMATION = "confirmation";

    public final String ADDRESS_CONFIRMATION;
    public final String ADDRESS_RESPONSE;

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

    private <T>T kafkaRequestResponse(StupidStreamObject request, Class<T> responseType) throws InterruptedException {
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            request);

        LOGGER.info("Waiting for response on channel with uuid " + offset);

        // Will block until a response is received
        String serializedResponse = this.multithreadedCommunication.consumeAndDestroy(offset);
        return Constants.gson.fromJson(serializedResponse, responseType);
    }

    private <T>T httpRequestResponse(String address, StupidStreamObject request, Class<T> responseType) {
        try {
            String serializedResponse =
                HttpUtils.httpRequestResponse(address, Constants.gson.toJson(request));
            LOGGER.info("Direct HTTP response received: " + serializedResponse);
            return Constants.gson.fromJson(serializedResponse, responseType);
        } catch (IOException e) {
            LOGGER.warning("Error when sending request to " + address +
                " in the storageapi http request response: " + e.getMessage());
            throw new RuntimeException(e);
        }
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
    /*public ResponseMessageDetails searchAndDetails(String searchText) throws InterruptedException {
        return kafkaRequestResponse(
            RequestSearchAndDetails.getStupidStreamObject(searchText, new Addressable(ADDRESS_RESPONSE)),
            ResponseMessageDetails.class);
    }*/

    public ResponseSearchMessage searchMessage(String searchText) {
        return httpRequestResponse(Constants.LUCENE_REQUEST_ADDRESS,
            RequestSearchMessage.getStupidStreamObject(searchText, new Addressable(ADDRESS_RESPONSE)),
            ResponseSearchMessage.class);
    }

    public ResponseMessageDetails messageDetails(Long uuid) {
        return httpRequestResponse(Constants.PSQL_REQUEST_ADDRESS,
            RequestMessageDetails.getStupidStreamObject(uuid, new Addressable(ADDRESS_RESPONSE)),
            ResponseMessageDetails.class);
    }

    public ResponseAllMessages allMessages() {
        return httpRequestResponse(Constants.PSQL_REQUEST_ADDRESS,
            new StupidStreamObject(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, new Addressable(ADDRESS_RESPONSE)),
            ResponseAllMessages.class
        );
    }

    // Writes
    public void postMessage(Message message) {
        LOGGER.info("Posting message " + message);
        KafkaUtils.produceMessage(this.producer,
            this.transactionsTopic,
            new RequestPostMessage(message, new Addressable(ADDRESS_CONFIRMATION)).toStupidStreamObject()
        );
    }

    public void deleteAllMessages() {
        KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            new StupidStreamObject(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES,
                new Addressable(ADDRESS_CONFIRMATION))
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
