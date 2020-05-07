import java.io.IOException;
import java.util.function.Consumer;

public abstract class StorageSystemFactory<T> implements AutoCloseable {
    final HttpStorageSystem httpStorageSystem;
    final SnapshottedDatabase<T> snapshottedWrapper;
    final Consumer<StorageSystem<T>> bootstrapProcedure;

    public StorageSystemFactory(String name,
                                SnapshottedDatabase<T> snapshottedWrapper,
                                int httpListenPort,
                                Consumer<StorageSystem<T>> bootstrapProcedure) throws IOException {
        this.snapshottedWrapper = snapshottedWrapper;
        this.httpStorageSystem = new HttpStorageSystem(name, HttpUtils.initHttpServer(httpListenPort));
        this.bootstrapProcedure = bootstrapProcedure;
    }

    abstract StorageSystem<T> simpleOlep();

    abstract StorageSystem<T> serReads();

    abstract StorageSystem<T> sdRequestNoSession();

    abstract StorageSystem<T> sdRequestSeparateSession();

    abstract StorageSystem<T> concurReads();

    @Override
    public void close() throws Exception {

    }
}
