import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class JointStorageSystem<Snap extends AutoCloseable> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(JointStorageSystem.class.getName());

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();
    private final Map<StupidStreamObject.ObjectType, ServiceBase<Snap>> kafkaServiceHandlers = new HashMap<>();
    private final Map<StupidStreamObject.ObjectType, ServiceBase<Snap>> httpServiceHandlers = new HashMap<>();

    final String fullName;
    final String shortName;
    private WrappedSnapshottedStorageSystem<Snap> wrapper;

    final private NamedTimeMeasurements processingTimeMeasurements;
    final private NamedTimeMeasurements totalTimeMeasurements;
    final private Counter waitForContactCounter;
    final private Meter completedOperations;
    final private SettableGauge<Long> waitingThreadsTime = new SettableGauge<>();
    final private Counter openedSnapshotsCounter;
    final private Counter waitingForSnapshotsCounter;

    // For concurrency, I'm splitting two types of resources - threads and connections TODO: Check more thoroughly
    final static private int LIMIT_DB_THREADS = 10;
    final static private int LIMIT_RESPONSE_THREADS = 5;
    final private ExecutorService databaseOpsExecutors = Executors.newFixedThreadPool(LIMIT_DB_THREADS);
    final private ExecutorService responseExecutors = Executors.newFixedThreadPool(LIMIT_RESPONSE_THREADS);

    final private boolean hasConnectionsLimit;
    final private List<Semaphore> semaphoreConnections = new ArrayList<>();

    public JointStorageSystem(String fullName,
                              ManualConsumer<Long, StupidStreamObject> eventStorageSystem,
                              HttpStorageSystem httpStorageSystem,
                              WrappedSnapshottedStorageSystem<Snap> wrapper,
                              int numberOfSemaphoreChannels) {
        this.fullName = fullName;
        this.shortName = Constants.getStorageSystemBaseName(fullName);
        this.wrapper = wrapper;

        this.hasConnectionsLimit = numberOfSemaphoreChannels > 0;
        if (hasConnectionsLimit) {
            int maxAllowedParallel = Math.min(LIMIT_DB_THREADS, wrapper.getMaxNumberOfSnapshots());

            int eachChannel = maxAllowedParallel / numberOfSemaphoreChannels;
            for (int i = 0; i < numberOfSemaphoreChannels; i++) {
                this.semaphoreConnections.add(new Semaphore(eachChannel));
            }
        }

        this.processingTimeMeasurements = new NamedTimeMeasurements(this.shortName);
        this.totalTimeMeasurements = new NamedTimeMeasurements(this.shortName + ".total-time");
        this.waitForContactCounter =
            Constants.METRIC_REGISTRY.counter(this.shortName + ".wait-for-contact");
        this.completedOperations = Constants.METRIC_REGISTRY.meter(this.shortName + ".completed-operations");
        Constants.METRIC_REGISTRY.register(this.shortName + ".wait-for-contact-time", waitingThreadsTime);
        this.openedSnapshotsCounter = Constants.METRIC_REGISTRY.counter(this.shortName + ".opened-snapshots");
        this.waitingForSnapshotsCounter =
            Constants.METRIC_REGISTRY.counter(this.shortName + ".waiting-for-snapshots");

        // Subscribe to kafka and http listeners
        eventStorageSystem.subscribe(this::kafkaServiceHandler);
        httpStorageSystem.registerHandler("query", this::httpServiceHandler);

        // Settle things for contact
        httpStorageSystem.registerHandler("contact", this::externalContact);
    }

    // Handlers
    private byte[] externalContact(String contactResponse) {
        multithreadedCommunication.registerResponse(contactResponse);
        return "Thanks!".getBytes();
    }

    private byte[] httpServiceHandler(String serializedQuery) {
        StupidStreamObject sso = Constants.gson.fromJson(serializedQuery, StupidStreamObject.class);

        LOGGER.info(String.format("%s received an HTTP query of type %s", fullName, sso.getObjectType()));

        requestArrived(this.httpServiceHandlers, sso, wrapResponseWithAddress(sso.getResponseAddress()));
        return "Thanks, processing... :)".getBytes();
    }

    private void kafkaServiceHandler(ConsumerRecord<Long, StupidStreamObject> record) {
        StupidStreamObject sso = record.value();
        LOGGER.info(String.format("%s received a Kafka query: %s", fullName, sso.getObjectType()));

        // The Kafka offset is only known after the message has been published
        sso.getResponseAddress().setChannelID(record.offset());

        requestArrived(this.kafkaServiceHandlers, sso, wrapResponseWithAddress(sso.getResponseAddress()));
    }

    private Consumer<MultithreadedResponse> wrapResponseWithAddress(Addressable responseAddress) {
        return (MultithreadedResponse response) -> respond(responseAddress, response);
    }

    private void respond(Addressable responseAddress, MultithreadedResponse response) {
        responseExecutors.submit(() -> {
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

    private Snap obtainConnection(ServiceBase<Snap> serviceHandler) {
        if (this.hasConnectionsLimit) {
            try {
                this.waitingForSnapshotsCounter.inc();
                LOGGER.info(String.format("Trying to acquire a connection on channel %d when there are %d available " +
                        "semaphore resources", serviceHandler.asyncHandleChannel,
                    this.semaphoreConnections.get(serviceHandler.asyncHandleChannel).availablePermits()));

                this.semaphoreConnections
                    .get(serviceHandler.asyncHandleChannel)
                    .acquire();

                this.waitingForSnapshotsCounter.dec();
            } catch (InterruptedException e) {
                LOGGER.warning("Error when trying to obtain a semaphore resource on channel " +
                    serviceHandler.asyncHandleChannel + ": " + e);
                throw new RuntimeException(e);
            }
        }

        openedSnapshotsCounter.inc();
        return wrapper.getConcurrentSnapshot();
    }

    private void releaseConnection(ServiceBase<Snap> serviceHandler, Snap snapshot) {
        try {
            snapshot.close();
        } catch (Exception e) {
            LOGGER.warning("Error when closing database connection: " + e);
            throw new RuntimeException(e);
        }

        if (this.hasConnectionsLimit) {
            this.semaphoreConnections
                .get(serviceHandler.asyncHandleChannel)
                .release();

            LOGGER.info(String.format("Released a connection sem on channel %d when there are %d available " +
                    "semaphore resources", serviceHandler.asyncHandleChannel,
                this.semaphoreConnections.get(serviceHandler.asyncHandleChannel).availablePermits()));
        }
        openedSnapshotsCounter.dec();
    }

    private void handleWithHandler(StupidStreamObject sso,
                                   ServiceBase<Snap> serviceHandler,
                                   Consumer<MultithreadedResponse> responseCallback) {
        String objectTypeStr = sso.getObjectType().toString();
        long uid = sso.getResponseAddress().getChannelID();

        totalTimeMeasurements.startTimer(objectTypeStr, uid);

        if (serviceHandler.asyncHandleChannel != -1) {
            Snap snapshotToUse = this.obtainConnection(serviceHandler);

            databaseOpsExecutors.submit(() -> {
                processingTimeMeasurements.startTimer(objectTypeStr, uid);

                try {
                    serviceHandler.handleRequest(sso, wrapper, responseCallback, this, snapshotToUse);
                    this.releaseConnection(serviceHandler, snapshotToUse);
                } catch (Exception e) {
                    LOGGER.warning("Error when closing the snapshot in storage system " + fullName);
                    throw new RuntimeException(e);
                }

                processingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
                totalTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
                completedOperations.mark();
            });
        } else {
            processingTimeMeasurements.startTimer(objectTypeStr, uid);

            LOGGER.info(fullName + " calls the handler for request of type " + sso.getObjectType());
            serviceHandler.handleRequest(sso, wrapper, responseCallback, this, wrapper.getDefaultSnapshot());

            processingTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
            totalTimeMeasurements.stopTimerAndPublish(objectTypeStr, uid);
            completedOperations.mark();
        }
    }

    private void requestArrived(Map<StupidStreamObject.ObjectType, ServiceBase<Snap>> serviceHandlers,
                                StupidStreamObject sso,
                                Consumer<MultithreadedResponse> responseCallback) {
        LOGGER.info("Request arrived for " + fullName + " of type " + sso.getObjectType());

        var serviceHandler = serviceHandlers.get(sso.getObjectType());

        if (serviceHandler == null) {
            LOGGER.info("Couldn't find a handler for request of type " + sso.getObjectType());
            return;
        }

        handleWithHandler(sso, serviceHandler, responseCallback);
    }

    protected <T> T waitForContact(long channel, Class<T> classOfResponse) {
        waitForContactCounter.inc();
        long startTime = System.nanoTime();

        String serialized;
        try {
            serialized = multithreadedCommunication.consumeAndDestroy(channel);
        } catch (InterruptedException e) {
            LOGGER.warning("Error in " + fullName + " while waiting on channel " + channel + " for external contact");
            throw new RuntimeException(e);
        }

        long elapsed = (System.nanoTime() - startTime) / 1000000;
        waitingThreadsTime.setValue(elapsed);
        waitForContactCounter.dec();
        return Constants.gson.fromJson(serialized, classOfResponse);
    }

    public JointStorageSystem<Snap> registerKafkaService(ServiceBase<Snap> serviceDescription) {
        this.kafkaServiceHandlers.put(serviceDescription.getObjectTypeToHandle(), serviceDescription);
        return this;
    }

    public JointStorageSystem<Snap> registerHttpService(ServiceBase<Snap> serviceDescription) {
        this.httpServiceHandlers.put(serviceDescription.getObjectTypeToHandle(), serviceDescription);
        return this;
    }

    @Override
    public void close() throws Exception {
        wrapper.getDefaultSnapshot().close();
    }
}
