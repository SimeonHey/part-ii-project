import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import io.vavr.Tuple2;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class StorageAPI implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(StorageAPI.class.getName());
    private static final String ENDPOINT_RESPONSE = "response";

    private final String responseAddress;

    private final Producer<Long, EventBase> producer;
    private final String transactionsTopic;

    private final ChanneledCommunication channeledCommunication = new ChanneledCommunication();
    private final HttpStorageSystem httpStorageSystem;

    private AtomicLong httpRequestsUid = new AtomicLong(-1); // Decreases to remove conflicts with Kafka ids

    private List<Long> confirmationChannelsList = new ArrayList<>(); // Keeps track of all writes' channel ids

    private BlockingQueue<ConfirmationListener> confirmationListeners = new LinkedBlockingQueue<>();

    private final List<Tuple2<String, List<String>>> httpFavours;

    private final NamedTimeMeasurements favoursTimeMeasurers = new NamedTimeMeasurements("favours");
    private final Counter outstandingFavoursCounter =
        Constants.METRIC_REGISTRY.counter("favours.outstanding-count");
    private final NamedMeterMeasurements opsSentMeters = new NamedMeterMeasurements("ops-sent-meters");

    private final Meter responsesReceivedMeter = Constants.METRIC_REGISTRY.meter("responses-received-meter");

    StorageAPI(Producer<Long, EventBase> producer,
               HttpStorageSystem httpStorageSystem,
               String transactionsTopic,
               String selfAddress,
               List<Tuple2<String, List<String>>> httpFavours) {
        this.producer = producer;
        this.transactionsTopic = transactionsTopic;
        this.httpStorageSystem = httpStorageSystem;

        this.httpFavours = httpFavours;

        this.httpStorageSystem.registerHandler(ENDPOINT_RESPONSE, this::receiveResponse);

        this.responseAddress =  String.format("%s/%s", httpStorageSystem.getFullAddress(selfAddress),
            ENDPOINT_RESPONSE);

        LOGGER.info("Address response: " + responseAddress);
    }

    private Tuple2<String, List<String>> findHttpFavour(EventBase request) {
        for (var tuple: httpFavours) {
            if (tuple._1.equals(request.getEventType())) {
                return tuple;
            }
        }

        return null;
    }

    public <T>CompletableFuture<T> handleRequest(EventBase request, Class<T> responseType) {
        outstandingFavoursCounter.inc();

        var httpFavors = findHttpFavour(request);

        opsSentMeters.markFor(request.getEventType());

        if (httpFavors == null) { // I assume if it's not in the HTTP it's in the Kafka favour group
            LOGGER.info("StorageAPI handles request " + request + " through Kafka");

            return kafkaRequestResponseFuture(request, responseType);
        } else {
            LOGGER.info("StorageAPI handles request " + request + " through HTTP to: " + httpFavors);
            // TODO: This is unnecessarily ugly
            return httpFavors._2
                .stream()
                .map(url -> httpRequestAsyncResponseFuture(url, request, responseType))
                .collect(Collectors.toList())
                .get(0);
        }
    }

    public void handleRequest(EventBase request) {
        handleRequest(request, Object.class);
    }

    private <T> CompletableFuture<T> consumeAndDestroyAsync(long offset, Class<T> responseType) {
        return CompletableFuture.supplyAsync(() -> {
            LOGGER.info("Waiting for response on channel with uuid " + offset);

            // Will block until a response is received
            T response;
            try {
                // In the case of multihop requests, we need to wait for the real result
                // I assume it's the first non-null
                String serializedResponse = this.channeledCommunication.consumeAndDestroy(offset, true);
                response = Constants.gson.fromJson(serializedResponse, responseType);

                outstandingFavoursCounter.dec();
            } catch (InterruptedException e) {
                LOGGER.warning("Error when waiting on channel " + offset + ": " + e);
                throw new RuntimeException(e);
            }

            LOGGER.info("Got a response for channel " + offset + ": " + response);
            return response;
        });
    }

    private <T> CompletableFuture<T> kafkaRequestResponseFuture(EventBase request, Class<T> responseType) {
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            request);

        favoursTimeMeasurers.startTimer(request.getEventType(), offset);

        if (request.expectsResponse()) {
            return consumeAndDestroyAsync(offset, responseType);
        } else {
            LOGGER.info("Sent request. Confirmation will arrive on channel " + offset);
            this.confirmationChannelsList.add(offset);
            return null;
        }
    }

    private <T>CompletableFuture<T> httpRequestAsyncResponseFuture(String address,
                                                                   EventBase request,
                                                                   Class<T> responseType) {
        long curId = httpRequestsUid.getAndDecrement();
        request.getResponseAddress().setChannelID(curId);
        favoursTimeMeasurers.startTimer(request.getEventType(), curId);

        try {
            HttpUtils.sendHttpRequest(address, Constants.gson.toJson(request));
        } catch (IOException e) {
            LOGGER.warning("Error when sending request through HTTP: " + e);
            throw new RuntimeException(e);
        }

        if (request.expectsResponse()) {
            return consumeAndDestroyAsync(curId, responseType);
        } else {
            LOGGER.info("Sent request. Confirmation will arrive on channel " + curId);
            this.confirmationChannelsList.add(curId);
            return null;
        }
    }

    public void registerConfirmationListener(ConfirmationListener confirmationListener) {
        confirmationListeners.add(confirmationListener);
    }

    public void clearConfirmationListeners() {
        LOGGER.info("Removing " + confirmationListeners.size() + " confirmation listeners");
        confirmationListeners.clear();
    }

    private byte[] receiveResponse(String serializedResponse) {
        LOGGER.info(String.format("StorageAPI received a thing with length %d", serializedResponse.length()));
        try {
            ChanneledResponse response = this.channeledCommunication.registerResponse(serializedResponse);
            responsesReceivedMeter.mark();
            LOGGER.info(String.format("The response with length %d; Response details: %s", serializedResponse.length(),
                response.toString()));
            favoursTimeMeasurers.stopTimerAndPublish(response.getRequestObjectType(), response.getChannelUuid());

            // Notify all confirmation listeners as this is a response anyways
            ConfirmationResponse confirmationResponse = new ConfirmationResponse(response.getFromStorageSystem(),
                response.getRequestObjectType());
            LOGGER.info("Notifying " + confirmationListeners.size() + " confirmation listeners");
            confirmationListeners.forEach(listener -> listener.receivedConfirmation(confirmationResponse));
        } catch (Exception e) {
            LOGGER.warning("Error while trying to register a response in the StorageAPI: " + e);
            throw new RuntimeException(e);
        }

        return ("Received response of length " + serializedResponse.length()).getBytes();
    }

    private void waitOnChannelForConfirmation(long channelID, boolean destroy) {
        LOGGER.info("Waiting for confirmations from at least one storage system on channel " + channelID + "...");
        try {
            if (destroy) {
                channeledCommunication.consumeAndDestroy(channelID, false);
            } else {
                channeledCommunication.consume(channelID, false);
            }
        } catch (InterruptedException e) {
            LOGGER.warning("Error while waiting on channel " + channelID);
            throw new RuntimeException(e);
        }
    }

    public void waitForAllConfirmations() {
        LOGGER.info("Waiting for " + confirmationChannelsList.size() + " confirmations from at least one storage system." +
            "..");

        confirmationChannelsList.forEach(id -> waitOnChannelForConfirmation(id, true));

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
