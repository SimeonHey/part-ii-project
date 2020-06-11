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

public class PolyglotAPI implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(PolyglotAPI.class.getName());
    private static final String ENDPOINT_RESPONSE = "response";

    private final String responseAddress;
    private final String selfAddress;

    private final Producer<Long, EventBase> producer;
    private final String transactionsTopic;

    private final ChanneledCommunication channeledCommunication = new ChanneledCommunication();
    private final HttpStorageSystem httpStorageSystem;

    private AtomicLong httpRequestsUid = new AtomicLong(-1); // Decreases to remove conflicts with Kafka ids

    private List<Long> confirmationChannelsList = new ArrayList<>(); // Keeps track of all writes' channel ids

    private BlockingQueue<ConfirmationListener> confirmationListeners = new LinkedBlockingQueue<>();

    private final List<Tuple2<String, List<String>>> httpActions;

    private final NamedTimeMeasurements requestsTimeMeasurers = new NamedTimeMeasurements("favours");
    private final Counter outstandingFavoursCounter =
        Constants.METRIC_REGISTRY.counter("favours.outstanding-count");
    private final NamedMeterMeasurements opsSentMeters = new NamedMeterMeasurements("ops-sent-meters");
    private final Meter opsSentAll = Constants.METRIC_REGISTRY.meter("ops-sent-all");

    private final Meter responsesReceivedMeter = Constants.METRIC_REGISTRY.meter("responses-received-meter");

    PolyglotAPI(Producer<Long, EventBase> producer,
                HttpStorageSystem httpStorageSystem,
                String transactionsTopic,
                String selfAddress,
                List<Tuple2<String, List<String>>> httpActions) {
        this.producer = producer;
        this.transactionsTopic = transactionsTopic;
        this.httpStorageSystem = httpStorageSystem;
        this.selfAddress = selfAddress;
        this.httpActions = httpActions;

        this.httpStorageSystem.registerHandler(ENDPOINT_RESPONSE, this::receiveResponse);

        this.responseAddress =  String.format("%s/%s", httpStorageSystem.getFullAddress(selfAddress),
            ENDPOINT_RESPONSE);

        LOGGER.info("Address response: " + responseAddress);
    }

    PolyglotAPI(int polyglotApiPort, String[] kafkaAddressTopic, String selfAddress,
                List<Tuple2<String, List<String>>> httpActions) {
        this.producer = KafkaUtils.createProducer(kafkaAddressTopic[0], "StorageApi");
        this.transactionsTopic = kafkaAddressTopic[1];
        this.httpStorageSystem = new HttpStorageSystem("PolyglotAPI",
            HttpUtils.initHttpServer(polyglotApiPort));
        this.selfAddress = selfAddress;
        this.httpActions = httpActions;
        this.responseAddress =  String.format("%s/%s", httpStorageSystem.getFullAddress(selfAddress),
            ENDPOINT_RESPONSE);

        this.httpStorageSystem.registerHandler(ENDPOINT_RESPONSE, this::receiveResponse);
    }

    private Tuple2<String, List<String>> findHttpFavour(EventBase request) {
        for (var tuple: httpActions) {
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
        opsSentAll.mark();

        if (httpFavors == null) { // I assume if it's not in the HTTP it's in the Kafka action group
            LOGGER.info("PolyglotAPI handles request " + request + " through Kafka");
            return kafkaRequestResponseFuture(request, responseType);
        } else {
            LOGGER.info("PolyglotAPI handles request " + request + " through HTTP to: " + httpFavors);
            return httpFavors._2
                .stream()
                .map(url -> httpRequestAsyncResponseFuture(url, request, responseType))
                .collect(Collectors.toList())
                .get(0);
        }
    }

    public CompletableFuture<ConfirmationResponse> handleRequest(EventBase request) {
        return handleRequest(request, ConfirmationResponse.class);
    }

    private <T> CompletableFuture<T> consumeAndDestroyAsync(long offset, Class<T> responseType, boolean isResponse) {
        return CompletableFuture.supplyAsync(() -> {
            LOGGER.info("Waiting for " + (isResponse ? "response" : "confirmation")
                + " on channel with uuid " + offset);

            // Will block until a response is received
            T response;
            try {
                // In the case of multihop requests, we need to wait for the real result
                // I assume it's the first non-null
                String serializedResponse = this.channeledCommunication.consumeAndDestroy(offset, isResponse);
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
        request.setResponseAddress(new Addressable(this.responseAddress)); // Don't set the ID here; the reciving storage
        // systems will
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            request);

        requestsTimeMeasurers.startTimer("psql." + request.getEventType(), offset);
        requestsTimeMeasurers.startTimer("lucene." + request.getEventType(), offset);
        requestsTimeMeasurers.startTimer("vavr." + request.getEventType(), offset);

        if (request.expectsResponse()) {
            return consumeAndDestroyAsync(offset, responseType, true);
        } else {
            LOGGER.info("Sent request. Confirmation will arrive on channel " + offset);
//            this.confirmationChannelsList.add(offset);
            return consumeAndDestroyAsync(offset, responseType, false);
        }
    }

    private <T>CompletableFuture<T> httpRequestAsyncResponseFuture(String address,
                                                                   EventBase request,
                                                                   Class<T> responseType) {
        long curId = httpRequestsUid.getAndDecrement();
        request.setResponseAddress(new Addressable(this.responseAddress, curId));
        requestsTimeMeasurers.startTimer("psql." + request.getEventType(), curId);
        requestsTimeMeasurers.startTimer("lucene." + request.getEventType(), curId);
        requestsTimeMeasurers.startTimer("vavr." + request.getEventType(), curId);

        try {
            HttpUtils.sendHttpRequest(address, Constants.gson.toJson(request));
        } catch (IOException e) {
            LOGGER.warning("Error when sending request through HTTP: " + e);
            throw new RuntimeException(e);
        }

        if (request.expectsResponse()) {
            return consumeAndDestroyAsync(curId, responseType, true);
        } else {
            LOGGER.info("Sent request. Confirmation will arrive on channel " + curId);
//            this.confirmationChannelsList.add(curId);
            return consumeAndDestroyAsync(curId, responseType, false);
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
        LOGGER.info(String.format("PolyglotAPI received a thing with length %d", serializedResponse.length()));
        try {
            ChanneledResponse response = this.channeledCommunication.registerResponse(serializedResponse);
            responsesReceivedMeter.mark();
            LOGGER.info(String.format("The thing with length %d; Response details: %s", serializedResponse.length(),
                response.toString()));

            // Only register it if it is a response (i.e the end of the request for multihops)
            requestsTimeMeasurers.stopTimerAndPublish(response.getFromStorageSystem() + "." +
                    response.getRequestObjectType(), response.getChannelUuid());


            // Notify all confirmation listeners as this is a response anyways
            ConfirmationResponse confirmationResponse = new ConfirmationResponse(response.getFromStorageSystem(),
                response.getRequestObjectType());
            LOGGER.info("Notifying " + confirmationListeners.size() + " confirmation listeners");
            confirmationListeners.forEach(listener -> listener.receivedConfirmation(confirmationResponse));
        } catch (Exception e) {
            LOGGER.warning("Error while trying to register a response in the PolyglotAPI: " + e);
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
        return "PolyglotAPI{" +
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
