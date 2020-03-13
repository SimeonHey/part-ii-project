import java.io.IOException;
import java.util.concurrent.ExecutorService;

public abstract class StorageSystemFactory<T extends AutoCloseable> implements AutoCloseable {
    final LoopingConsumer<Long, StupidStreamObject> loopingKafka;
    final HttpStorageSystem httpStorageSystem;
    final ExecutorService executorServiceForLoopingListener;
    final WrappedSnapshottedStorageSystem<T> snapshottedWrapper;

    private void initProcedure() {
        this.snapshottedWrapper.deleteAllMessages();
        this.executorServiceForLoopingListener.submit(this.loopingKafka::listenBlockingly);
    }

    public StorageSystemFactory(String name,
                                ExecutorService executorServiceForLoopingListener,
                                WrappedSnapshottedStorageSystem<T> snapshottedWrapper,
                                int httpListenPort) throws IOException {
        this.executorServiceForLoopingListener = executorServiceForLoopingListener;
        this.snapshottedWrapper = snapshottedWrapper;

        this.loopingKafka =
            new LoopingConsumer<>(KafkaUtils.createConsumer(name,
                Constants.KAFKA_ADDRESS, Constants.KAFKA_TOPIC));
        this.httpStorageSystem = new HttpStorageSystem(name,
            HttpUtils.initHttpServer(httpListenPort));

        initProcedure();
    }

    abstract JointStorageSystem<T> simpleOlep();

    abstract JointStorageSystem<T> serReads();

    @Override
    public void close() throws Exception {
        executorServiceForLoopingListener.shutdownNow();
    }
}
