import com.codahale.metrics.Counter;
import io.vavr.Tuple2;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class StorageAPI implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(StorageAPI.class.getName());
    private static final String ENDPOINT_RESPONSE = "response";

    private final String responseAddress;

    private final Producer<Long, BaseEvent> producer;
    private final String transactionsTopic;

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();
    private final HttpStorageSystem httpStorageSystem;

    private AtomicLong httpRequestsUid = new AtomicLong(0); // Decreases to remove conflicts with Kafka ids

    private List<Long> confirmationChannelsList = new ArrayList<>(); // Keeps track of all writes' channel ids

    private List<ConfirmationListener> confirmationListeners = new ArrayList<>();

    private final List<Tuple2<String, String>> httpFavours;

    private final NamedTimeMeasurements favoursTimeMeasurers = new NamedTimeMeasurements("favours");
    private final Counter outstandingFavoursCounter =
        Constants.METRIC_REGISTRY.counter("favours.outstanding-count");

    StorageAPI(Producer<Long, BaseEvent> producer,
               HttpStorageSystem httpStorageSystem,
               String transactionsTopic,
               String selfAddress,
               List<Tuple2<String, String>> httpFavours) {
        this.producer = producer;
        this.transactionsTopic = transactionsTopic;
        this.httpStorageSystem = httpStorageSystem;

        this.httpFavours = httpFavours;

        this.httpStorageSystem.registerHandler(ENDPOINT_RESPONSE, this::receiveResponse);

        this.responseAddress =  String.format("%s/%s", httpStorageSystem.getFullAddress(selfAddress),
            ENDPOINT_RESPONSE);

        LOGGER.info("Address response: " + responseAddress);
    }

    private Tuple2<String, String> findHttpFavour(BaseEvent request) {
        for (var tuple: httpFavours) {
            if (tuple._1.equals(request.getObjectType())) {
                return tuple;
            }
        }

        return null;
    }

    public <T>CompletableFuture<T> handleRequest(BaseEvent request, Class<T> responseType) {
        outstandingFavoursCounter.inc();

        var httpFavor = findHttpFavour(request);

        if (httpFavor == null) { // I assume if it's not in the HTTP it's in the Kafka favour group
            LOGGER.info("StorageAPI handles request " + request + " through Kafka");

            return kafkaRequestResponseFuture(request, responseType);
        } else {
            LOGGER.info("StorageAPI handles request " + request + " through HTTP");

            return httpRequestAsyncResponseFuture(httpFavor._2, request, responseType);
        }
    }

    public void handleRequest(BaseEvent request) {
        handleRequest(request, Object.class);
    }

    private <T> CompletableFuture<T> consumeAndDestroyAsync(long offset, Class<T> responseType) {
        return CompletableFuture.supplyAsync(() -> {
            // Will block until a response is received
            String serializedResponse;
            try {
                serializedResponse = this.multithreadedCommunication.consumeAndDestroy(offset);
                outstandingFavoursCounter.dec();
            } catch (InterruptedException e) {
                LOGGER.warning("Error when waiting on channel " + offset + ": " + e);
                throw new RuntimeException(e);
            }
            return Constants.gson.fromJson(serializedResponse, responseType);
        });
    }

    private <T> CompletableFuture<T> kafkaRequestResponseFuture(BaseEvent request, Class<T> responseType) {
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            request);

        if (request.expectsResponse()) {
            LOGGER.info("Waiting for response on channel with uuid " + offset);

            favoursTimeMeasurers.startTimer(request.getObjectType(), offset);
            return consumeAndDestroyAsync(offset, responseType);
        } else {
            this.confirmationChannelsList.add(offset);
            return null;
        }
    }

    private <T>CompletableFuture<T> httpRequestAsyncResponseFuture(String address, BaseEvent request,
                                                                   Class<T> responseType) {
        try {
            long curId = httpRequestsUid.getAndDecrement();
            request.getResponseAddress().setChannelID(curId);

            LOGGER.info(String.format("Sending a request of type %s through HTTP with id = %d, and waiting for " +
                    "response on that channel...", request.getObjectType(), curId));

            if (!request.expectsResponse()) {
                this.confirmationChannelsList.add(curId);
            }

            favoursTimeMeasurers.startTimer(request.getObjectType(), curId);
            HttpUtils.sendHttpRequest(address, Constants.gson.toJson(request));

            return consumeAndDestroyAsync(curId, responseType);
        } catch (IOException e) {
            LOGGER.warning("Error when sending request to " + address +
                " in the storageapi http request: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void registerConfirmationListener(ConfirmationListener confirmationListener) {
        confirmationListeners.add(confirmationListener);
    }

    private byte[] receiveResponse(String serializedResponse) {
        LOGGER.info(String.format("Received response with length %d", serializedResponse.length()));
        try {
            MultithreadedResponse response = this.multithreadedCommunication.registerResponse(serializedResponse);
            LOGGER.info(String.format("The response with length %d; Response details: %s", serializedResponse.length(),
                response.toString()));
            favoursTimeMeasurers.stopTimerAndPublish(response.getRequestObjectType(), response.getChannelUuid());

            // Notify all confirmation listeners as this is a response anyways
            ConfirmationResponse confirmationResponse = new ConfirmationResponse(response.getFromStorageSystem(),
                response.getRequestObjectType());
            confirmationListeners.forEach(listener -> listener.receivedConfirmation(confirmationResponse));
        } catch (Exception e) {
            LOGGER.warning("Error while trying to register a response in the StorageAPI: " + e);
            throw new RuntimeException(e);
        }
        return ("Received response of length " + serializedResponse.length()).getBytes();
    }

    private void waitOnChannel(long channelID, boolean destroy) {
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

    public String getResponseAddress() {
        return responseAddress;
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
        LOGGER.info("Closing the Storage API...");

        this.waitForAllConfirmations();
        httpStorageSystem.close();
    }
}
