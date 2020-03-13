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

    private final String name;
    private WrappedSnapshottedStorageSystem<Snap> wrapper;

    public JointStorageSystem(String name,
                              SubscribableConsumer<Long, StupidStreamObject> eventStorageSystem,
                              HttpStorageSystem httpStorageSystem,
                              WrappedSnapshottedStorageSystem<Snap> wrapper) {
        this.name = name;
        this.wrapper = wrapper;

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

        LOGGER.info(String.format("%s received an HTTP query of type %s", name, sso.getObjectType()));

        requestArrived(this.httpServiceHandlers, sso, wrapResponseWithAddress(sso.getResponseAddress()));
        return "Thanks, processing... :)".getBytes();
    }

    private void kafkaServiceHandler(ConsumerRecord<Long, StupidStreamObject> record) {
        StupidStreamObject sso = record.value();
        LOGGER.info(String.format("%s received a Kafka query: %s", name, sso.getObjectType()));

        // The Kafka offset is only known after the message has been published
        sso.getResponseAddress().setChannelID(record.offset());

        requestArrived(this.kafkaServiceHandlers, sso, wrapResponseWithAddress(sso.getResponseAddress()));
    }

    private Consumer<MultithreadedResponse> wrapResponseWithAddress(Addressable responseAddress) {
        return (MultithreadedResponse response) -> respond(responseAddress, response);
    }

    private void respond(Addressable responseAddress, MultithreadedResponse response) {
        LOGGER.info(name + " joint storage system responds to " + responseAddress + ": " + response);
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
            new Thread(() ->
                serviceHandler.handleRequest(sso, wrapper, responseCallback, this)
            ).start();
        } else {
            LOGGER.info(name + " calls the handler for request of type " + sso.getObjectType());
            serviceHandler.handleRequest(sso, wrapper, responseCallback, this);
        }
    }

    private void requestArrived(List<ServiceBase<Snap>> serviceHandlers,
                                StupidStreamObject sso,
                                Consumer<MultithreadedResponse> responseCallback) {
        LOGGER.info("Request arrived for " + name + " of type " + sso.getObjectType());
        for (ServiceBase<Snap> serviceHandler: serviceHandlers) {
            if (serviceHandler.couldHandle(sso)) {
                LOGGER.info(name + " found a handler for request of type " + sso.getObjectType());
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
        String serialized;
        try {
            serialized = multithreadedCommunication.consumeAndDestroy(channel);
        } catch (InterruptedException e) {
            LOGGER.warning("Error in " + name + " while waiting on channel " + channel + " for external contact");
            throw new RuntimeException(e);
        }
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
