import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class JointStorageSystemBuilder<Snap> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(JointStorageSystemBuilder.class.getName());

    private final Map<String, ActionBase<Snap>> serviceHandlers = new HashMap<>();

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

    public JointStorageSystemBuilder<Snap> registerAction(ActionBase<Snap> actionHandler) {
        if (this.classMap.containsKey(actionHandler.getEventTypeToHandle())) {
            throw new RuntimeException("Duplicated action " + actionHandler.getEventTypeToHandle());
        }

        int number = this.classMap.size();
        this.classMap.put(actionHandler.getEventTypeToHandle(), actionHandler.getClassOfObjectToHandle());
        this.classNumber.put(actionHandler.getEventTypeToHandle(), number);

        this.serviceHandlers.put(actionHandler.getEventTypeToHandle(), actionHandler);
        LOGGER.info("Registered a Kafka service: " + actionHandler);
        return this;
    }

    public JointStorageSystem<Snap> build() {
        LOGGER.info("Building a storage system " + fullName + "...");
        // Construct the storage system
        var storageSystem = new JointStorageSystem<>(fullName, wrapper, serviceHandlers, classMap, classNumber,
            new MultithreadedEventQueueExecutor(10,
                //new MultithreadedEventQueueExecutor.FifoScheduler()),
                 new MultithreadedEventQueueExecutor.StaticChannelsScheduler(classMap.size())), // A chennel for each
            new MultithreadedEventQueueExecutor(2, new MultithreadedEventQueueExecutor.FifoScheduler()));

        LOGGER.info("Registering handlers...");
        // Subscribe to http listeners
        httpStorageSystem.registerHandler("query", storageSystem::httpActionHandler);
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
