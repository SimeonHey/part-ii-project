import java.io.IOException;
import java.util.concurrent.ExecutorService;

public abstract class StorageSystemFactory<T extends AutoCloseable> implements AutoCloseable {
    final LoopingConsumer<Long, StupidStreamObject> kafka;
    final HttpStorageSystem httpStorageSystem;
    final WrappedSnapshottedStorageSystem<T> snapshottedWrapper;

    private void initProcedure() {
        this.snapshottedWrapper.deleteAllMessages();
    }

    public StorageSystemFactory(String name,
                                WrappedSnapshottedStorageSystem<T> snapshottedWrapper,
                                int httpListenPort,
                                LoopingConsumer<Long, StupidStreamObject> kafka) throws IOException {
        this.snapshottedWrapper = snapshottedWrapper;

        this.kafka = kafka;
        this.httpStorageSystem = new HttpStorageSystem(name,
            HttpUtils.initHttpServer(httpListenPort));

        initProcedure();
    }

    public void listenBlockingly(ExecutorService executorServiceForLoopingListener) {
        executorServiceForLoopingListener.submit(this.kafka::listenBlockingly);
    }

    public ManualConsumer<Long, StupidStreamObject> getManualConsumer() {
        return kafka;
    }

    abstract JointStorageSystem<T> simpleOlep();

    abstract JointStorageSystem<T> serReads();

    abstract JointStorageSystem<T> sdRequestNoSession();

    abstract JointStorageSystem<T> sdRequestSeparateSession();

    @Override
    public void close() throws Exception {

    }
}
