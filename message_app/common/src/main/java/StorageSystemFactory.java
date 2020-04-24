import java.io.IOException;
import java.util.function.Consumer;

public abstract class StorageSystemFactory<T> implements AutoCloseable {
    final HttpStorageSystem httpStorageSystem;
    final SnapshottedStorageSystem<T> snapshottedWrapper;
    final Consumer<JointStorageSystem<T>> bootstrapProcedure;

    public StorageSystemFactory(String name,
                                SnapshottedStorageSystem<T> snapshottedWrapper,
                                int httpListenPort,
                                Consumer<JointStorageSystem<T>> bootstrapProcedure) throws IOException {
        this.snapshottedWrapper = snapshottedWrapper;
        this.httpStorageSystem = new HttpStorageSystem(name, HttpUtils.initHttpServer(httpListenPort));
        this.bootstrapProcedure = bootstrapProcedure;
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
