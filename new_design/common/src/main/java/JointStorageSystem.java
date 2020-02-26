import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class JointStorageSystem<Snap extends AutoCloseable> {
    // TODO: Logging
    private WrappedSnapshottedStorageSystem<Snap> wrapper;

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();
    private final List<ServiceBase<Snap>> serviceHandlers = new ArrayList<>();

    public JointStorageSystem(SubscribableConsumer<Long, StupidStreamObject> eventStorageSystem,
                              HttpStorageSystem httpStorageSystem,
                              WrappedSnapshottedStorageSystem<Snap> wrapper) {
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
        requestArrived(sso, this::respond);
        return "Processing request...".getBytes();
    }

    private void kafkaServiceHandler(ConsumerRecord<Long, StupidStreamObject> record) {
        requestArrived(record.value(), this::respond);
    }

    private void respond(StupidStreamObject response) {
        String serialized = Constants.gson.toJson(response);
        try {
            HttpUtils.sendHttpRequest(response.getResponseAddress(), serialized);
        } catch (IOException e) {

            throw new RuntimeException(e);
        }
    }

    private void handleWithHandler(StupidStreamObject sso,
                                                 ServiceBase<Snap> serviceHandler,
                                                 Consumer<StupidStreamObject> responseCallback) {
        if (serviceHandler.handleAsyncWithSnapshot) {
            new Thread(() ->
                serviceHandler.handleRequest(sso, wrapper, responseCallback)
            ).start();
        } else {
            serviceHandler.handleRequest(sso, wrapper, responseCallback);
        }
    }

    private void requestArrived(StupidStreamObject sso, Consumer<StupidStreamObject> responseCallback) {
        for (ServiceBase<Snap> serviceHandler: this.serviceHandlers) {
            if (serviceHandler.couldHandle(sso)) {
                handleWithHandler(sso, serviceHandler, responseCallback);
            }
        }

        // TODO: This might be okay, so would have no need for an exception
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
}