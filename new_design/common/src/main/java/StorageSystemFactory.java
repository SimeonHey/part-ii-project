import java.io.IOException;
import java.util.concurrent.ExecutorService;

public abstract class StorageSystemFactory<T extends AutoCloseable> {
    final LoopingConsumer<Long, StupidStreamObject> loopingKafka;
    final HttpStorageSystem httpStorageSystem;
    final ExecutorService executorService;
    final WrappedSnapshottedStorageSystem<T> snapshottedWrapper;

    private void initProcedure() {
        this.snapshottedWrapper.deleteAllMessages();
    }

    public StorageSystemFactory(String name,
                                ExecutorService executorService,
                                WrappedSnapshottedStorageSystem<T> snapshottedWrapper,
                                int httpListenPort) throws IOException {
        this.executorService = executorService;
        this.snapshottedWrapper = snapshottedWrapper;

        this.loopingKafka =
            new LoopingConsumer<>(KafkaUtils.createConsumer(name,
                Constants.KAFKA_ADDRESS, Constants.KAFKA_TOPIC));
        this.httpStorageSystem = new HttpStorageSystem(name,
            HttpUtils.initHttpServer(httpListenPort));

        initProcedure();
    }

    abstract JointStorageSystem<T> simpleOlep() throws Exception;
}
