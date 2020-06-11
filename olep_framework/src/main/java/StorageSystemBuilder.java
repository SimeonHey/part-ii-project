import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class StorageSystemBuilder<Snap> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(StorageSystemBuilder.class.getName());

    private final Map<String, ActionBase<Snap>> serviceHandlers = new HashMap<>();

    private final Map<String, Class<? extends EventBase>> classMap = new HashMap<>();
    private final Map<String, Integer> classNumber = new HashMap<>();

    private final String fullName;
    private final HttpStorageSystem httpStorageSystem;
    private final SnapshottedDatabase<Snap> wrapper;
    private final Consumer<StorageSystem<Snap>> kafkaConsumerSubscription;

    public StorageSystemBuilder(String fullName,
                                HttpStorageSystem httpStorageSystem,
                                SnapshottedDatabase<Snap> wrapper,
                                Consumer<StorageSystem<Snap>> kafkaConsumerSubscription) {
        this.fullName = fullName;
        this.httpStorageSystem = httpStorageSystem;
        this.wrapper = wrapper;
        this.kafkaConsumerSubscription = kafkaConsumerSubscription;
    }

    public StorageSystemBuilder(String fullName,
                                SnapshottedDatabase<Snap> wrapper,
                                int httpListenPort,
                                String[] addressTopic) {
        this.fullName = fullName;
        this.httpStorageSystem = new HttpStorageSystem(fullName, HttpUtils.initHttpServer(httpListenPort));
        this.wrapper = wrapper;
        this.kafkaConsumerSubscription = (storageSystem) -> {
            var consumer = new LoopingConsumer(
                storageSystem.fullName,
                addressTopic[0],
                addressTopic[1],
                storageSystem.classMap);
            consumer.moveAllToLatest();
            consumer.subscribe(storageSystem::kafkaActionHandler);
            Executors.newFixedThreadPool(1).submit(consumer::listenBlockingly);
        };
    }

    public StorageSystemBuilder<Snap> registerAction(ActionBase<Snap> actionHandler) {
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

    public StorageSystem<Snap> buildAndRun() {
        LOGGER.info("Building a storage system " + fullName + "...");
        // Construct the storage system
        var storageSystem = new StorageSystem<>(fullName, wrapper, serviceHandlers, classMap, classNumber,
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
