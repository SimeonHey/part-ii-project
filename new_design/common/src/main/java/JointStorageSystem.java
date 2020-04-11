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
    private final SnapshottedStorageWrapper<Snap> wrapper;

    private final Map<String, ServiceBase<Snap>> kafkaServiceHandlers;
    private final Map<String, ServiceBase<Snap>> httpServiceHandlers;

    final Map<String, Class<? extends BaseEvent>> classMap;
    private final Map<String, Integer> classNumber;

    private final NamedTimeMeasurements processingTimeMeasurements;
    private final NamedTimeMeasurements totalProcessingTimeMeasurements;
    private final Counter waitForContactCounter;
    private final TimeMeasurer waitingThreadsTimeMeasurer;

    private final Counter openedSnapshotsCounter;
    private final Counter waitForSnapshotCounter;

    private final Meter completedOperations;
    private final Meter operationsReceived;

    private final MultithreadedEventQueueExecutor databaseOpsExecutors;
    private final MultithreadedEventQueueExecutor responseExecutors;

    JointStorageSystem(String fullName,
                       HttpStorageSystem httpStorageSystem,
                       SnapshottedStorageWrapper<Snap> wrapper,
                       Map<String, ServiceBase<Snap>> kafkaServiceHandlers,
                       Map<String, ServiceBase<Snap>> httpServiceHandlers,
                       Map<String, Class<? extends BaseEvent>> classMap,
                       Map<String, Integer> classNumber,
                       MultithreadedEventQueueExecutor databaseOpsExecutors,
                       MultithreadedEventQueueExecutor responseExecutors) {
        this.fullName = fullName;
        this.shortName = Constants.getStorageSystemBaseName(fullName);
        this.wrapper = wrapper;

        this.kafkaServiceHandlers = kafkaServiceHandlers;
        this.httpServiceHandlers = httpServiceHandlers;

        this.classMap = classMap;
        this.classNumber = classNumber;

        this.databaseOpsExecutors = databaseOpsExecutors;
        this.responseExecutors = responseExecutors;

        // Subscribe to kafka and http listeners
        httpStorageSystem.registerHandler("query", this::httpServiceHandler);

        // Settle things for contact
        httpStorageSystem.registerHandler("contact", this::externalContact);

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
    }

    // Handlers
    private byte[] externalContact(String contactResponse) {
        channeledCommunication.registerResponse(contactResponse);
        return "Thanks!".getBytes();
    }

    private byte[] httpServiceHandler(String serializedQuery) {
        LOGGER.info(fullName + " received an HTTP query of length " + serializedQuery.length() +
            ", deserializing...");

        BaseEvent sso = EventJsonDeserializer.deserialize(Constants.gson, serializedQuery, classMap);

        LOGGER.info(String.format("%s received an HTTP query of type %s", fullName, sso.getEventType()));

        requestArrived(this.httpServiceHandlers, sso, wrapResponseWithAddress(sso.getResponseAddress()));
        return "Thanks, processing... :)".getBytes();
    }

    void kafkaServiceHandler(ConsumerRecord<Long, BaseEvent> record) {
        BaseEvent sso = record.value();
        LOGGER.info(String.format("%s received a Kafka query: %s", fullName, sso.getEventType()));

        // The Kafka offset is only known after the message has been published
        sso.getResponseAddress().setChannelID(record.offset());

        requestArrived(this.kafkaServiceHandlers, sso, wrapResponseWithAddress(sso.getResponseAddress()));
    }

    private Consumer<ChanneledResponse> wrapResponseWithAddress(Addressable responseAddress) {
        return (ChanneledResponse response) -> respond(responseAddress, response);
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

    private void handleWithHandler(BaseEvent event,
                                   ServiceBase<Snap> serviceHandler,
                                   Consumer<ChanneledResponse> responseCallback) {
        LOGGER.info(this.fullName + " found a handler for event type " + event.getEventType());

        String objectTypeStr = event.getEventType();
        long uid = event.getResponseAddress().getChannelID();

        totalProcessingTimeMeasurements.startTimer(objectTypeStr, uid);

        if (serviceHandler.asyncHandleChannel != -1) {
            SnapshotHolder<Snap> snapshotToUse = this.obtainConcurrentSnapshot();
            // Execute asynchronously
            int number = classNumber.get(event.getEventType());
            databaseOpsExecutors.submitOperation(number, () -> {
                processingTimeMeasurements.startTimer(objectTypeStr, uid);

                try {
                    serviceHandler.handleRequest(event, responseCallback, this, snapshotToUse.getSnapshot());
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
                serviceHandler.handleRequest(event, responseCallback, this, wrapper.getDefaultSnapshot());
            } catch (Exception e) {
                LOGGER.warning("Error when handling request: " + e);
                throw new RuntimeException(e);
            }

            processingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
            totalProcessingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
            completedOperations.mark();
        }
    }

    private void requestArrived(Map<String, ServiceBase<Snap>> serviceHandlers,
                                BaseEvent baseEvent,
                                Consumer<ChanneledResponse> responseCallback) {
        operationsReceived.mark();
        LOGGER.info("Request arrived for " + fullName + " of type " + baseEvent.getEventType());

        var serviceHandler = serviceHandlers.get(baseEvent.getEventType());

        if (serviceHandler == null) {
            LOGGER.warning("Couldn't find a handler for request of type " + baseEvent.getEventType());
            return;
        }

        handleWithHandler(baseEvent, serviceHandler, responseCallback);
        LOGGER.info("Successfully processed (but possibly not completed) request of type " + baseEvent.getEventType());
    }

    protected <T> T waitForContact(long channel, Class<T> classOfResponse) {
        waitForContactCounter.inc();
        TimeMeasurer.ActiveTimer activeTimer = waitingThreadsTimeMeasurer.startTimer();

        String serialized;
        try {
            serialized = channeledCommunication.consumeAndDestroy(channel);
        } catch (InterruptedException e) {
            LOGGER.warning("Error in " + fullName + " while waiting on channel " + channel + " for external contact");
            throw new RuntimeException(e);
        }

        waitingThreadsTimeMeasurer.stopTimerAndPublish(activeTimer);
        waitForContactCounter.dec();
        return Constants.gson.fromJson(serialized, classOfResponse);
    }

    @Override
    public String toString() {
        return "JointStorageSystem{" +
            "fullName='" + fullName + '\'' +
            ", wrapper=" + wrapper +
            ", kafkaServiceHandlers=" + kafkaServiceHandlers.keySet() +
            ", httpServiceHandlers=" + httpServiceHandlers.keySet() +
            ", classMap=" + classMap.keySet() +
            ", databaseOpsExecutors=" + databaseOpsExecutors +
            ", responseExecutors=" + responseExecutors +
            '}';
    }

    @Override
    public void close() throws Exception {

    }
}
