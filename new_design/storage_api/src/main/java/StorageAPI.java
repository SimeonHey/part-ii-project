import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    private final MultithreadedCommunication multithreadedCommunication;
    private final HttpStorageSystem httpStorageSystem;

    private long httpRequestsUid = 0; // Decreases to remove conflicts with Kafka ids

    private List<Long> confirmationChannelsList = new ArrayList<>(); // Keeps track of all writes' channel ids

    StorageAPI(Producer<Long, StupidStreamObject> producer,
               HttpStorageSystem httpStorageSystem,
               String transactionsTopic) {
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

    private <T>T kafkaRequestResponse(StupidStreamObject request, Class<T> responseType) {
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            request);


        LOGGER.info("Waiting for response on channel with uuid " + offset);

        // Will block until a response is received
        String serializedResponse;
        try {
            serializedResponse = this.multithreadedCommunication.consumeAndDestroy(offset);
        } catch (InterruptedException e) {
            LOGGER.warning("Error when waiting on channel " + offset + ": " + e);
            throw new RuntimeException(e);
        }
        return Constants.gson.fromJson(serializedResponse, responseType);
    }

    private <T> CompletableFuture<T> kafkaRequestResponseFuture(StupidStreamObject request, Class<T> responseType) {
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            request);

        LOGGER.info("Waiting for response on channel with uuid " + offset);

        return CompletableFuture.supplyAsync(() -> {
            // Will block until a response is received
            String serializedResponse;
            try {
                serializedResponse = this.multithreadedCommunication.consumeAndDestroy(offset);
            } catch (InterruptedException e) {
                LOGGER.warning("Error when waiting on channel " + offset + ": " + e);
                throw new RuntimeException(e);
            }
            return Constants.gson.fromJson(serializedResponse, responseType);
        });
    }

    private <T>T httpRequestResponse(String address, StupidStreamObject request, Class<T> responseType) {
        try {
            // TODO : Needs more care in the multithreaded case. MVP
            long curId = httpRequestsUid;
            httpRequestsUid -= 1;
            request.getResponseAddress().setChannelID(curId);

            String serializedResponse =
                HttpUtils.httpRequestResponse(address, Constants.gson.toJson(request));
            MultithreadedResponse response =
                Constants.gson.fromJson(serializedResponse, MultithreadedResponse.class);

            LOGGER.info("Direct HTTP response received: " + response);
            return Constants.gson.fromJson(response.getSerializedResponse(), responseType);
        } catch (IOException e) {
            LOGGER.warning("Error when sending request to " + address +
                " in the storageapi http request response: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private <T>T httpRequestMultithreadedResponse(String address, StupidStreamObject request, Class<T> responseType) {
        try {
            // TODO : Needs more care in the multithreaded case. MVP
            long curId = httpRequestsUid;
            httpRequestsUid -= 1;
            request.getResponseAddress().setChannelID(curId);

            LOGGER.info(String.format("Sending a request of type %s through HTTP with id = %d...",
                request.getObjectType().toString(), curId));
            HttpUtils.httpRequestResponse(address, Constants.gson.toJson(request));

            String serializedResponse = multithreadedCommunication.consumeAndDestroy(curId); // This blocks
            LOGGER.info(String.format("Received serialized response %s", serializedResponse));

            return Constants.gson.fromJson(serializedResponse, responseType);
        } catch (IOException | InterruptedException e) {
            LOGGER.warning("Error when sending request to " + address +
                " in the storageapi http request: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }


    private byte[] receiveResponse(String serializedResponse) {
        LOGGER.info(String.format("Received response %s", serializedResponse));

        try {
            this.multithreadedCommunication.registerResponse(serializedResponse);
        } catch (Exception e) {
            LOGGER.warning("Error while trying to register a response in the StorageAPI: " + e);
            throw new RuntimeException(e);
        }
        return ("Received response " + serializedResponse).getBytes();
    }

    private byte[] receiveConfirmation(String serializedResponse) {
        LOGGER.info(String.format("Received confirmation: %s", serializedResponse));
        try {
            multithreadedCommunication.registerResponse(serializedResponse);
        } catch (Exception e) {
            LOGGER.warning("Error while trying to register a confirmation in the StorageAPI: " + e);
            throw new RuntimeException(e);
        }
        return ("Thanks!").getBytes();
    }

    // Reads
    /*public ResponseMessageDetails searchAndDetails(String searchText) throws InterruptedException {
        return kafkaRequestResponse(
            RequestSearchAndDetails.getStupidStreamObject(searchText, new Addressable(ADDRESS_RESPONSE)),
            ResponseMessageDetails.class);
    }*/

    public ResponseSearchMessage searchMessage(String searchText) {
        return kafkaRequestResponse(//Constants.LUCENE_REQUEST_ADDRESS,
            RequestSearchMessage.getStupidStreamObject(searchText, new Addressable(ADDRESS_RESPONSE)),
            ResponseSearchMessage.class);
    }

    public ResponseMessageDetails messageDetails(Long uuid) {
        return kafkaRequestResponse(//Constants.PSQL_REQUEST_ADDRESS,
            RequestMessageDetails.getStupidStreamObject(uuid, new Addressable(ADDRESS_RESPONSE)),
            ResponseMessageDetails.class);
    }

    public ResponseAllMessages allMessages() {
        return kafkaRequestResponse(//Constants.PSQL_REQUEST_ADDRESS,
            new StupidStreamObject(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, new Addressable(ADDRESS_RESPONSE)),
            ResponseAllMessages.class
        );
    }

    public ResponseMessageDetails searchAndDetails(String searchText) {
        return kafkaRequestResponse(//Constants.LUCENE_REQUEST_ADDRESS,
            RequestSearchAndDetails.getStupidStreamObject(searchText, new Addressable(ADDRESS_RESPONSE)),
            ResponseMessageDetails.class);
    }

    public CompletableFuture<ResponseMessageDetails> searchAndDetailsFuture(String searchText) {
        return kafkaRequestResponseFuture(//Constants.LUCENE_REQUEST_ADDRESS,
            RequestSearchAndDetails.getStupidStreamObject(searchText, new Addressable(ADDRESS_RESPONSE)),
            ResponseMessageDetails.class);
    }

    // Writes
    public long postMessage(Message message) {
        LOGGER.info("Posting message " + message);
        long curId = KafkaUtils.produceMessage(this.producer,
            this.transactionsTopic,
            new RequestPostMessage(message, new Addressable(ADDRESS_CONFIRMATION)).toStupidStreamObject()
        );

        confirmationChannelsList.add(curId);
        return curId;
    }

    public long deleteAllMessages() {
        long curId = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            new StupidStreamObject(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES,
                new Addressable(ADDRESS_CONFIRMATION))
        );

        confirmationChannelsList.add(curId);
        return curId;
    }

    public void waitOnChannel(long channelID, boolean destroy) {
        try {
            if (destroy) {
                multithreadedCommunication.consumeAndDestroy(channelID);
            } else {
                multithreadedCommunication.consume(channelID);
            }
        } catch (InterruptedException e) {
            LOGGER.warning("Error while waiting on channel " + channelID);
            throw new RuntimeException(e);
        }
    }

    public void waitForAllConfirmations() {
        LOGGER.info("Waiting for confirmations from all storage systems...");

        for (int i=0; i<Constants.NUM_STORAGE_SYSTEMS; i++) {
            boolean last = (i+1) == Constants.NUM_STORAGE_SYSTEMS;
            confirmationChannelsList.forEach(id -> waitOnChannel(id, last));
        }

        confirmationChannelsList.clear();
        LOGGER.info("Received confirmations and cleared the current list.");
    }

    @Override
    public String toString() {
        return "StorageAPI{" +
            "producer=" + producer +
            ", transactionsTopic='" + transactionsTopic + '\'' +
            '}';
    }

    @Override
    public void close() throws Exception {
        this.waitForAllConfirmations();
        httpStorageSystem.close();
    }
}
;