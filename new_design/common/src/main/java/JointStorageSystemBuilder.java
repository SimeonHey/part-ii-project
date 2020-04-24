import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class JointStorageSystemBuilder<Snap> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(JointStorageSystemBuilder.class.getName());

    private final Map<String, ServiceBase<Snap>> serviceHandlers = new HashMap<>();

    private final Map<String, Class<? extends EventBase>> classMap = new HashMap<>();
    private final Map<String, Integer> classNumber = new HashMap<>();

    private final String fullName;
    private final HttpStorageSystem httpStorageSystem;
    private final SnapshottedStorageSystem<Snap> wrapper;
    private final Consumer<JointStorageSystem<Snap>> kafkaConsumerSubscription;

    public JointStorageSystemBuilder(String fullName,
                                     HttpStorageSystem httpStorageSystem,
                                     SnapshottedStorageSystem<Snap> wrapper,
                                     Consumer<JointStorageSystem<Snap>> kafkaConsumerSubscription) {
        this.fullName = fullName;
        this.httpStorageSystem = httpStorageSystem;
        this.wrapper = wrapper;
        this.kafkaConsumerSubscription = kafkaConsumerSubscription;
    }

    public JointStorageSystemBuilder<Snap> registerAction(ServiceBase<Snap> serviceDescription) {
        int number = this.classMap.size();
        this.classMap.put(serviceDescription.getEventTypeToHandle(), serviceDescription.getClassOfObjectToHandle());
        this.classNumber.put(serviceDescription.getEventTypeToHandle(), number);

        this.serviceHandlers.put(serviceDescription.getEventTypeToHandle(), serviceDescription);
        LOGGER.info("Registered a Kafka service: " + serviceDescription);
        return this;
    }

    public JointStorageSystem<Snap> build() {
        // Construct the storage system
        var storageSystem = new JointStorageSystem<>(fullName, wrapper, serviceHandlers, classMap, classNumber,
            new MultithreadedEventQueueExecutor(classMap.size(),
                new MultithreadedEventQueueExecutor.StaticChannelsScheduler(classMap.size())),
            new MultithreadedEventQueueExecutor(2, new MultithreadedEventQueueExecutor.FifoScheduler()));

        // Subscribe to http listeners
        httpStorageSystem.registerHandler("query", storageSystem::httpServiceHandler);
        httpStorageSystem.registerHandler("contact", storageSystem::externalContact);

        // Subscribe to Kafka
        this.kafkaConsumerSubscription.accept(storageSystem);

        LOGGER.info("Built storage system: " + storageSystem);
        return storageSystem;
    }

    @Override
    public void close() throws Exception {

    }
}
