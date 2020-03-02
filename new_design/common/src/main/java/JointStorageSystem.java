import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class JointStorageSystem<Snap extends AutoCloseable> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(JointStorageSystem.class.getName());
    private WrappedSnapshottedStorageSystem<Snap> wrapper;

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();
    private final List<ServiceBase<Snap>> serviceHandlers = new ArrayList<>();
    private final String name;

    public JointStorageSystem(String name,
                              SubscribableConsumer<Long, StupidStreamObject> eventStorageSystem,
                              HttpStorageSystem httpStorageSystem,
                              WrappedSnapshottedStorageSystem<Snap> wrapper) {
        this.name = name;
        this.wrapper = wrapper;

        // Subscribe to listeners
        eventStorageSystem.subscribe(this::kafkaServiceHandler);
        httpStorageSystem.registerHandler("contact", this::externalContact);
        httpStorageSystem.registerHandler("query", this::httpServiceHandler);
    }

    private byte[] externalContact(String contactResponse) {
        multithreadedCommunication.registerResponse(contactResponse);
        return "Thanks!".getBytes();
    }

    private byte[] httpServiceHandler(String serializedQuery) {
        StupidStreamObject sso = Constants.gson.fromJson(serializedQuery, StupidStreamObject.class);
        requestArrived(sso, wrapResponseWithAddress(sso.getResponseAddress()));
        return "Processing request...".getBytes();
    }

    private void kafkaServiceHandler(ConsumerRecord<Long, StupidStreamObject> record) {
        StupidStreamObject sso = record.value();
        sso.getResponseAddress().setChannelID(record.offset());

        requestArrived(sso, wrapResponseWithAddress(record.value().getResponseAddress()));
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

            throw new RuntimeException(e);
        }
    }

    private void handleWithHandler(StupidStreamObject sso,
                                   ServiceBase<Snap> serviceHandler,
                                   Consumer<MultithreadedResponse> responseCallback) {
        if (serviceHandler.handleAsyncWithSnapshot) {
            new Thread(() ->
                serviceHandler.handleRequest(sso, wrapper, responseCallback)
            ).start();
        } else {
            serviceHandler.handleRequest(sso, wrapper, responseCallback);
        }
    }

    private void requestArrived(StupidStreamObject sso, Consumer<MultithreadedResponse> responseCallback) {
        LOGGER.info(name + " joint storage system handles request of type " + sso.getObjectType());
        for (ServiceBase<Snap> serviceHandler: this.serviceHandlers) {
            if (serviceHandler.couldHandle(sso)) {
                handleWithHandler(sso, serviceHandler, responseCallback);
                return;
            }
        }

        LOGGER.warning("Couldn't find a handler for request of type " + sso.getObjectType());
        // TODO: This might be okay, so would have no need for an exception
        // TODO: Throwing an exception here will blow up the http server and won't show up in the logs
        throw new RuntimeException("No relevant handler for object type " + sso.getObjectType());
    }

    private <T> T waitForContact(long channel, Class<T> classOfResponse) throws InterruptedException {
        String serialized = multithreadedCommunication.consumeAndDestroy(channel);
        return Constants.gson.fromJson(serialized, classOfResponse);
    }

    public JointStorageSystem<Snap> registerService(ServiceBase<Snap> serviceDescription) {
        this.serviceHandlers.add(serviceDescription);
        return this;
    }

    @Override
    public void close() throws Exception {
        wrapper.getDefaultSnapshot().close();
    }
}
