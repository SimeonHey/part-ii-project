import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class JointStorageSystem<Snap extends AutoCloseable> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(JointStorageSystem.class.getName());

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();

    final String fullName;
    final String shortName;
    private final WrappedSnapshottedStorageSystem<Snap> wrapper;

    private final Map<String, ServiceBase<Snap>> kafkaServiceHandlers;
    private final Map<String, ServiceBase<Snap>> httpServiceHandlers;

    final Map<String, Class<? extends BaseEvent>> classMap;
    private final Map<String, Integer> classNumber;

    private final NamedTimeMeasurements processingTimeMeasurements;
    private final NamedTimeMeasurements totalTimeMeasurements;
    private final Counter waitForContactCounter;
    private final Meter completedOperations;
    private final TimeMeasurer waitingThreadsTimeMeasurer;
    private final Counter openedSnapshotsCounter;

    private static final int LIMIT_DB_THREADS = 4;
    private static final int LIMIT_RESPONSE_THREADS = 1;

    private final MultithreadedEventQueueExecutor databaseOpsExecutors =
        new MultithreadedEventQueueExecutor(LIMIT_DB_THREADS,
            new MultithreadedEventQueueExecutor.StaticChannelsScheduler(6));

    private final MultithreadedEventQueueExecutor responseExecutors =
        new MultithreadedEventQueueExecutor(LIMIT_RESPONSE_THREADS, new MultithreadedEventQueueExecutor.FifoScheduler());

    JointStorageSystem(String fullName,
                       HttpStorageSystem httpStorageSystem,
                       WrappedSnapshottedStorageSystem<Snap> wrapper,
                       Map<String, ServiceBase<Snap>> kafkaServiceHandlers,
                       Map<String, ServiceBase<Snap>> httpServiceHandlers,
                       Map<String, Class<? extends BaseEvent>> classMap,
                       Map<String, Integer> classNumber) {
        this.fullName = fullName;
        this.shortName = Constants.getStorageSystemBaseName(fullName);
        this.wrapper = wrapper;

        this.kafkaServiceHandlers = kafkaServiceHandlers;
        this.httpServiceHandlers = httpServiceHandlers;

        this.classMap = classMap;
        this.classNumber = classNumber;

        // Subscribe to kafka and http listeners
        httpStorageSystem.registerHandler("query", this::httpServiceHandler);

        // Settle things for contact
        httpStorageSystem.registerHandler("contact", this::externalContact);

        // Metrics below
        this.processingTimeMeasurements = new NamedTimeMeasurements(this.shortName);
        this.totalTimeMeasurements = new NamedTimeMeasurements(this.shortName + ".total-time");
        this.waitForContactCounter =
            Constants.METRIC_REGISTRY.counter(this.shortName + ".wait-for-contact");
        this.completedOperations = Constants.METRIC_REGISTRY.meter(this.shortName + ".completed-operations");
        this.waitingThreadsTimeMeasurer =
            new TimeMeasurer(Constants.METRIC_REGISTRY, this.shortName + ".wait-for-contact-time");
        this.openedSnapshotsCounter = Constants.METRIC_REGISTRY.counter(this.shortName + ".opened-snapshots");
    }

    // Handlers
    private byte[] externalContact(String contactResponse) {
        multithreadedCommunication.registerResponse(contactResponse);
        return "Thanks!".getBytes();
    }

    private byte[] httpServiceHandler(String serializedQuery) {
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

    private Consumer<MultithreadedResponse> wrapResponseWithAddress(Addressable responseAddress) {
        return (MultithreadedResponse response) -> respond(responseAddress, response);
    }

    private void respond(Addressable responseAddress, MultithreadedResponse response) {
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

    private Snap obtainConnection() {
        openedSnapshotsCounter.inc();
        return wrapper.getConcurrentSnapshot();
    }

    private void releaseConnection(Snap snapshot) {
        try {
            snapshot.close();
        } catch (Exception e) {
            LOGGER.warning("Error when closing database connection: " + e);
            throw new RuntimeException(e);
        }
        openedSnapshotsCounter.dec();
    }

    private void handleWithHandler(BaseEvent event,
                                   ServiceBase<Snap> serviceHandler,
                                   Consumer<MultithreadedResponse> responseCallback) {
        String objectTypeStr = event.getEventType();
        long uid = event.getResponseAddress().getChannelID();

        totalTimeMeasurements.startTimer(objectTypeStr, uid);

        if (serviceHandler.asyncHandleChannel != -1) {
            Snap snapshotToUse = this.obtainConnection();
            // Execute asynchronously
            int number = classNumber.get(event.getEventType());
            databaseOpsExecutors.submitOperation(number, () -> {
                processingTimeMeasurements.startTimer(objectTypeStr, uid);

                try {
                    serviceHandler.handleRequest(event, wrapper, responseCallback, this, snapshotToUse);
                } catch (Exception e) {
                    LOGGER.warning("Error when handling request: " + e);
                    throw new RuntimeException(e);
                }

                this.releaseConnection(snapshotToUse);

                processingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
                totalTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
                completedOperations.mark();
            });
        } else {
            processingTimeMeasurements.startTimer(objectTypeStr, uid);

            // Execute in the current thread
            try {
                serviceHandler.handleRequest(event, wrapper, responseCallback, this, wrapper.getDefaultSnapshot());
            } catch (Exception e) {
                LOGGER.warning("Error when handling request: " + e);
                throw new RuntimeException(e);
            }

            processingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
            totalTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
            completedOperations.mark();
        }
    }

    private void requestArrived(Map<String, ServiceBase<Snap>> serviceHandlers,
                                BaseEvent baseEvent,
                                Consumer<MultithreadedResponse> responseCallback) {
        LOGGER.info("Request arrived for " + fullName + " of type " + baseEvent.getEventType());

        var serviceHandler = serviceHandlers.get(baseEvent.getEventType());

        if (serviceHandler == null) {
            LOGGER.info("Couldn't find a handler for request of type " + baseEvent.getEventType());
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
            serialized = multithreadedCommunication.consumeAndDestroy(channel);
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
        wrapper.getDefaultSnapshot().close();
    }
}
