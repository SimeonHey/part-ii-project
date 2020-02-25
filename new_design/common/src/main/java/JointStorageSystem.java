import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;

public class JointStorageSystem<Snap extends AutoCloseable> {
    private WrappedSnapshottedStorageSystem<Snap> wrapper;
    private SubscribableConsumer<Long, StupidStreamObject> eventStorageSystem;
    private HttpStorageSystem httpStorageSystem;

    private final MultithreadedCommunication multithreadedCommunication = new MultithreadedCommunication();
    private final List<ServiceBase<Snap>> serviceHandlers = new ArrayList<>();

    public JointStorageSystem(SubscribableConsumer<Long, StupidStreamObject> eventStorageSystem,
                              HttpStorageSystem httpStorageSystem,
                              WrappedSnapshottedStorageSystem<Snap> wrapper) {
        this.eventStorageSystem = eventStorageSystem;
        this.httpStorageSystem = httpStorageSystem;
        this.wrapper = wrapper;

        // Subscribe to listeners
        this.eventStorageSystem.subscribe(this::kafkaServiceHandler);
        this.httpStorageSystem.registerHandler("contact", this::externalContact);
        this.httpStorageSystem.registerHandler("query", this::httpServiceHandler);
    }

    byte[] externalContact(String contactResponse) {
        multithreadedCommunication.registerResponse(contactResponse);
        return "Thanks!".getBytes();
    }

    byte[] httpServiceHandler(String serializedQuery) {
        StupidStreamObject sso = Constants.gson.fromJson(serializedQuery, StupidStreamObject.class);
        StupidStreamObject response = requestArrived(sso);
        return Constants.gson.toJson(response).getBytes();
    }

    public void kafkaServiceHandler(ConsumerRecord<Long, StupidStreamObject> record) {
        StupidStreamObject response = requestArrived(record.value());
    }

    private StupidStreamObject requestArrived(StupidStreamObject sso) {
        for (ServiceBase<Snap> serviceHandler: this.serviceHandlers) {
            if (serviceHandler.couldHandle(sso)) {
                return serviceHandler.handleRequest(sso, wrapper);
            }
        }

        // TODO: This might be okay
        throw new RuntimeException("No relevant handler for object type " + sso.getObjectType());
    }

    private <T> T waitForContact(long channel, Class<T> classOfResponse) throws InterruptedException {
        String serialized = multithreadedCommunication.consumeAndDestroy(channel);
        return Constants.gson.fromJson(serialized, classOfResponse);
    }

    public JointStorageSystem<Snap> registerService(ServiceBase<Snap> serviceDescription) {
        return this;
    }
}