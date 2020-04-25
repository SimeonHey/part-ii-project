import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class JointStorageSystem<Snap> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(JointStorageSystem.class.getName());

    private final ChanneledCommunication channeledCommunication = new ChanneledCommunication();

    final String fullName;
    final String shortName;
    private final SnapshottedStorageSystem<Snap> wrapper;

    private final Map<String, ActionBase<Snap>> actionHandlers;

    final Map<String, Class<? extends EventBase>> classMap;
    private final Map<String, Integer> classNumber;

    private final NamedTimeMeasurements processingTimeMeasurements;
    private final NamedTimeMeasurements totalProcessingTimeMeasurements;
    private final Counter waitForContactCounter;
    private final TimeMeasurer waitingThreadsTimeMeasurer;

    private final Counter openedSnapshotsCounter;
    private final Counter waitForSnapshotCounter;

    private final Meter completedOperations;
    private final Meter operationsReceived;

    private final SettableGauge<Long> lastOffsetAcceptedGauge;
    private final SettableGauge<Long> lastOffsetProcessedGauge;

    private final MultithreadedEventQueueExecutor databaseOpsExecutors;
    private final MultithreadedEventQueueExecutor responseExecutors;

    JointStorageSystem(String fullName,
                       SnapshottedStorageSystem<Snap> wrapper,
                       Map<String, ActionBase<Snap>> actionHandlers,
                       Map<String, Class<? extends EventBase>> classMap,
                       Map<String, Integer> classNumber,
                       MultithreadedEventQueueExecutor databaseOpsExecutors,
                       MultithreadedEventQueueExecutor responseExecutors) {
        this.fullName = fullName;
        this.shortName = Constants.getStorageSystemBaseName(fullName);
        this.wrapper = wrapper;

        this.actionHandlers = actionHandlers;

        this.classMap = classMap;
        this.classNumber = classNumber;

        this.databaseOpsExecutors = databaseOpsExecutors;
        this.responseExecutors = responseExecutors;

        // Metrics below
        this.processingTimeMeasurements = new NamedTimeMeasurements(this.shortName);
        this.totalProcessingTimeMeasurements = new NamedTimeMeasurements(this.shortName + ".total-time");
        this.waitForContactCounter =
            Constants.METRIC_REGISTRY.counter(this.shortName + ".wait-for-contact");
        this.completedOperations = Constants.METRIC_REGISTRY.meter(this.shortName + ".completed-operations");
        this.operationsReceived = Constants.METRIC_REGISTRY.meter(this.shortName + ".operations-received");
        this.waitingThreadsTimeMeasurer =
            new TimeMeasurer(Constants.METRIC_REGISTRY, this.shortName + ".wait-for-contact-time");
        this.openedSnapshotsCounter = Constants.METRIC_REGISTRY.counter(this.shortName + ".opened-snapshots");
        waitForSnapshotCounter = Constants.METRIC_REGISTRY.counter(this.shortName + ".wait-for-snapshot-meter");
        this.lastOffsetAcceptedGauge = new SettableGauge<>();
        Constants.METRIC_REGISTRY.register(this.shortName + ".last-offset-accepted", this.lastOffsetAcceptedGauge);
        this.lastOffsetProcessedGauge = new SettableGauge<>();
        Constants.METRIC_REGISTRY.register(this.shortName + ".last-offset-processed", this.lastOffsetProcessedGauge);
    }

    // Handlers
    byte[] externalContact(String contactResponse) {
        channeledCommunication.registerResponse(contactResponse);
        return "Thanks!".getBytes();
    }

    byte[] httpActionHandler(String serializedQuery) {
        LOGGER.info(fullName + " received an HTTP query of length " + serializedQuery.length() +
            ", deserializing...");

        EventBase event = EventJsonDeserializer.deserialize(Constants.gson, serializedQuery, classMap);

        LOGGER.info(String.format("%s received an HTTP query of type %s", fullName, event.getEventType()));

        requestArrived(event, wrapResponseWithMeta(event));
        return "Thanks, processing... :)".getBytes();
    }

    void kafkaActionHandler(ConsumerRecord<Long, EventBase> record) {
        EventBase event = record.value();
        LOGGER.info(String.format("%s received a Kafka query: %s", fullName, event.getEventType()));

        // The Kafka offset is only known after the message has been published
        event.getResponseAddress().setChannelID(record.offset());
        lastOffsetAcceptedGauge.setValue(record.offset());

        requestArrived(event, wrapResponseWithMeta(event));
    }

    private Consumer<Response> wrapResponseWithMeta(EventBase event) {
        return (Response bareResponse) -> {
            lastOffsetProcessedGauge.setValue(event.getResponseAddress().getChannelID());

            LOGGER.info(this.fullName + " sending response " + bareResponse);
            respond(event.getResponseAddress(), new ChanneledResponse(
                this.shortName,
                event.getEventType(),
                event.getResponseAddress().getChannelID(),
                bareResponse.getResponseToSendBack(),
                bareResponse.isResponse(),
                event.expectsResponse()));
        };
    }

    private void respond(Addressable responseAddress, ChanneledResponse response) {
        responseExecutors.submitOperation(() -> {
            LOGGER.info(fullName + " joint storage system responds to " + responseAddress + ": " + response);

            String serialized = Constants.gson.toJson(response);
            try {
                HttpUtils.sendHttpRequest(responseAddress.getInternetAddress(), serialized);
            } catch (IOException e) {
                LOGGER.warning("Error when responding to " + responseAddress + ": " + e);
                throw new RuntimeException(e);
            }
        });
    }

    private SnapshotHolder<Snap> obtainConcurrentSnapshot() {
        waitForSnapshotCounter.inc();
        var snapshot = wrapper.getConcurrentSnapshot();

        waitForSnapshotCounter.dec();
        openedSnapshotsCounter.inc(); // say you got it after you get it
        return snapshot;
    }

    private void releaseSnapshot(SnapshotHolder<Snap> snapshotHolder) {
        try {
            snapshotHolder.close();
        } catch (Exception e) {
            LOGGER.warning("Error when releasing a snapshot: " + e);
            throw new RuntimeException(e);
        }
        openedSnapshotsCounter.dec();
    }

    private void handleWithHandler(EventBase event,
                                   ActionBase<Snap> actionHandler,
                                   Consumer<Response> responseCallback) {
        LOGGER.info(this.fullName + " found a handler for event type " + event.getEventType());

        String objectTypeStr = event.getEventType();
        long uid = event.getResponseAddress().getChannelID();

        totalProcessingTimeMeasurements.startTimer(objectTypeStr, uid);

        if (actionHandler.asyncHandleChannel != -1) {
            SnapshotHolder<Snap> snapshotToUse = this.obtainConcurrentSnapshot();
            // Execute asynchronously
            int number = classNumber.get(event.getEventType());
            databaseOpsExecutors.submitOperation(number, () -> {
                processingTimeMeasurements.startTimer(objectTypeStr, uid);

                try {
                    Response response = actionHandler.handleEvent(event, this, snapshotToUse.getSnapshot());
                    responseCallback.accept(response);
                } catch (Exception e) {
                    LOGGER.warning("Error when handling request: " + e);
                    throw new RuntimeException(e);
                }

                this.releaseSnapshot(snapshotToUse);

                processingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
                totalProcessingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
                completedOperations.mark();
            });
        } else {
            processingTimeMeasurements.startTimer(objectTypeStr, uid);

            // Execute in the current thread
            try {
                Response response = actionHandler.handleEvent(event, this, wrapper.getDefaultSnapshot());
                responseCallback.accept(response);
            } catch (Exception e) {
                LOGGER.warning("Error when handling request: " + e);
                throw new RuntimeException(e);
            }

            processingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
            totalProcessingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
            completedOperations.mark();
        }
    }

    private void requestArrived(EventBase eventBase,
                                Consumer<Response> responseCallback) {
        operationsReceived.mark();
        LOGGER.info("Request arrived for " + fullName + " of type " + eventBase.getEventType());

        var actionHandler = actionHandlers.get(eventBase.getEventType());

        if (actionHandler == null) {
            LOGGER.info("Couldn't find a handler for request of type " + eventBase.getEventType());
            return;
        }

        handleWithHandler(eventBase, actionHandler, responseCallback);
        LOGGER.info("Successfully processed (but possibly not completed) request of type " + eventBase.getEventType());
    }

    protected <T> T waitForContact(long channel, Class<T> classOfResponse) {
        waitForContactCounter.inc();
        TimeMeasurer.ActiveTimer activeTimer = waitingThreadsTimeMeasurer.startTimer();

        String serialized;
        try {
            serialized = channeledCommunication.consumeAndDestroy(channel, true);
        } catch (InterruptedException e) {
            LOGGER.warning("Error in " + fullName + " while waiting on channel " + channel + " for external contact");
            throw new RuntimeException(e);
        }

        waitingThreadsTimeMeasurer.stopTimerAndPublish(activeTimer);
        waitForContactCounter.dec();
        return Constants.gson.fromJson(serialized, classOfResponse);
    }

    protected void nextHopContact(String contactAddress, EventBase originalRequest, EventBase nextHopRequest) {
        LOGGER.info(this.fullName + " contacts " + contactAddress + " with nextHopRequest " + nextHopRequest + " for " +
            "originalRequest " + originalRequest);

        String serialized = Constants.gson.toJson(
            new ChanneledResponse(shortName,
                originalRequest.getEventType(),
                originalRequest.getResponseAddress().getChannelID(),
                nextHopRequest,
                true,
                true));

        try {
            HttpUtils.sendHttpRequest(contactAddress, serialized);
        } catch (IOException e) {
            LOGGER.warning("Error when trying to contact " + contactAddress + " for next hop of the request");
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "JointStorageSystem{" +
            "fullName='" + fullName + '\'' +
            ", wrapper=" + wrapper +
            ", actionHandlers=" + actionHandlers.keySet() +
            ", classMap=" + classMap.keySet() +
            ", databaseOpsExecutors=" + databaseOpsExecutors +
            ", responseExecutors=" + responseExecutors +
            '}';
    }

    @Override
    public void close() throws Exception {
        this.wrapper.close();
        this.databaseOpsExecutors.close();
        this.responseExecutors.close();
    }
}
