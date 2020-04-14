import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class JointStorageSystemBuilder<Snap> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(JointStorageSystemBuilder.class.getName());

    private final Map<String, ServiceBase<Snap>> serviceHandlers = new HashMap<>();

    private final Map<String, Class<? extends BaseEvent>> classMap = new HashMap<>();
    private final Map<String, Integer> classNumber = new HashMap<>();

    private final String fullName;
    private final HttpStorageSystem httpStorageSystem;
    private final SnapshottedStorageWrapper<Snap> wrapper;
    private final Consumer<JointStorageSystem<Snap>> bootstrapProcedure;

    public JointStorageSystemBuilder(String fullName,
                                     HttpStorageSystem httpStorageSystem,
                                     SnapshottedStorageWrapper<Snap> wrapper,
                                     Consumer<JointStorageSystem<Snap>> bootstrapProcedure) {
        this.fullName = fullName;
        this.httpStorageSystem = httpStorageSystem;
        this.wrapper = wrapper;
        this.bootstrapProcedure = bootstrapProcedure;
    }

    public JointStorageSystemBuilder<Snap> registerService(ServiceBase<Snap> serviceDescription) {
        int number = this.classMap.size();
        this.classMap.put(serviceDescription.getObjectTypeToHandle(), serviceDescription.getClassOfObjectToHandle());
        this.classNumber.put(serviceDescription.getObjectTypeToHandle(), number);

        this.serviceHandlers.put(serviceDescription.getObjectTypeToHandle(), serviceDescription);
        LOGGER.info("Registered a Kafka service: " + serviceDescription);
        return this;
    }

    public JointStorageSystem<Snap> build() {
        // Construct the storage system
        var storageSystem = new JointStorageSystem<>(fullName, httpStorageSystem, wrapper, serviceHandlers,
            classMap, classNumber,
            new MultithreadedEventQueueExecutor(classMap.size(),
                new MultithreadedEventQueueExecutor.StaticChannelsScheduler(classMap.size())),
            new MultithreadedEventQueueExecutor(10, new MultithreadedEventQueueExecutor.FifoScheduler()));

        LOGGER.info("Built storage system: " + storageSystem);

        LOGGER.info("Running the user defined bootstrap procedure");
        this.bootstrapProcedure.accept(storageSystem);

        return storageSystem;
    }

    @Override
    public void close() throws Exception {

    }
}
