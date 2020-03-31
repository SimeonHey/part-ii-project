import java.io.IOException;
import java.util.function.Consumer;

public abstract class StorageSystemFactory<T extends AutoCloseable> implements AutoCloseable {
    final HttpStorageSystem httpStorageSystem;
    final WrappedSnapshottedStorageSystem<T> snapshottedWrapper;
    final Consumer<JointStorageSystem<T>> bootstrapProcedure;

    private void initProcedure() {
        this.snapshottedWrapper.deleteAllMessages();
    }

    public StorageSystemFactory(String name,
                                WrappedSnapshottedStorageSystem<T> snapshottedWrapper,
                                int httpListenPort,
                                Consumer<JointStorageSystem<T>> bootstrapProcedure) throws IOException {
        this.snapshottedWrapper = snapshottedWrapper;
        this.httpStorageSystem = new HttpStorageSystem(name, HttpUtils.initHttpServer(httpListenPort));
        this.bootstrapProcedure = bootstrapProcedure;

        initProcedure();
    }

    abstract JointStorageSystem<T> simpleOlep();

    abstract JointStorageSystem<T> serReads();

    abstract JointStorageSystem<T> sdRequestNoSession();

    abstract JointStorageSystem<T> sdRequestSeparateSession();

    abstract JointStorageSystem<T> concurReads();

    @Override
    public void close() throws Exception {

    }
}
