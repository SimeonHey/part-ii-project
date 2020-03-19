import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class JointStorageSystem<Snap extends AutoCloseable> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(JointStorageSystem.class.getName());

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();
    private final List<ServiceBase<Snap>> kafkaServiceHandlers = new ArrayList<>();
    private final List<ServiceBase<Snap>> httpServiceHandlers = new ArrayList<>();

    final String fullName;
    final String shortName;
    private WrappedSnapshottedStorageSystem<Snap> wrapper;

    final private NamedTimeMeasurements namedTimeMeasurements;
    final private Counter waitForContactCounter;
    final private Meter completedOperations;
    final private SettableGauge<Long> waitingThreadsTime = new SettableGauge<>();

    public JointStorageSystem(String fullName,
                              ManualConsumer<Long, StupidStreamObject> eventStorageSystem,
                              HttpStorageSystem httpStorageSystem,
                              WrappedSnapshottedStorageSystem<Snap> wrapper) {
        this.fullName = fullName;
        this.shortName = Constants.getStorageSystemBaseName(fullName);
        this.wrapper = wrapper;

        this.namedTimeMeasurements = new NamedTimeMeasurements(this.shortName);
        this.waitForContactCounter =
            Constants.METRIC_REGISTRY.counter(this.shortName + ".wait-for-contact");
        this.completedOperations = Constants.METRIC_REGISTRY.meter(this.shortName + ".completed-operations");
        Constants.METRIC_REGISTRY.register(this.shortName + ".wait-for-contact-time", waitingThreadsTime);

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
        LOGGER.info(fullName + " joint storage system responds to " + responseAddress + ": " + response);
        String serialized = Constants.gson.toJson(response);
        try {
            HttpUtils.httpRequestResponse(responseAddress.getInternetAddress(), serialized);
        } catch (IOException e) {
            LOGGER.warning("Error when responding to " + responseAddress + ": " + e);
            throw new RuntimeException(e);
        }
    }

    private void handleWithHandler(StupidStreamObject sso,
                                   ServiceBase<Snap> serviceHandler,
                                   Consumer<MultithreadedResponse> responseCallback) {
        if (serviceHandler.handleAsyncWithSnapshot) {
            Snap snapshotToUse = wrapper.getConcurrentSnapshot();
            new Thread(() -> {
                namedTimeMeasurements.startTimer(sso.getObjectType().toString());

                try {
                    serviceHandler.handleRequest(sso, wrapper, responseCallback, this, snapshotToUse);
                    snapshotToUse.close();
                } catch (Exception e) {
                    LOGGER.warning("Error when closing the snapshot in storage system " + fullName);
                    throw new RuntimeException(e);
                }

                namedTimeMeasurements.stopTimerAndPublish(sso.getObjectType().toString());
                completedOperations.mark();
            }).start();
        } else {
            namedTimeMeasurements.startTimer(sso.getObjectType().toString());

            LOGGER.info(fullName + " calls the handler for request of type " + sso.getObjectType());
            serviceHandler.handleRequest(sso, wrapper, responseCallback, this, null);

            namedTimeMeasurements.stopTimerAndPublish(sso.getObjectType().toString());
            completedOperations.mark();
        }
    }

    private void requestArrived(List<ServiceBase<Snap>> serviceHandlers,
                                StupidStreamObject sso,
                                Consumer<MultithreadedResponse> responseCallback) {
        LOGGER.info("Request arrived for " + fullName + " of type " + sso.getObjectType());
        for (ServiceBase<Snap> serviceHandler: serviceHandlers) {
            if (serviceHandler.couldHandle(sso)) {
                LOGGER.info(fullName + " found a handler for request of type " + sso.getObjectType());
                handleWithHandler(sso, serviceHandler, responseCallback);
                return;
            }
        }

        LOGGER.warning("Couldn't find a handler for request of type " + sso.getObjectType());
        // TODO: This might be okay, so would have no need for an exception
        // TODO: Throwing an exception here will blow up the http server and won't show up in the logs
//        throw new RuntimeException("No relevant handler for object type " + sso.getObjectType());
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
        this.kafkaServiceHandlers.add(serviceDescription);
        return this;
    }

    public JointStorageSystem<Snap> registerHttpService(ServiceBase<Snap> serviceDescription) {
        this.httpServiceHandlers.add(serviceDescription);
        return this;
    }

    @Override
    public void close() throws Exception {
        wrapper.getDefaultSnapshot().close();
    }
}
