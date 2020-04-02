import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

public class SnapshotHolder<T> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(SnapshotHolder.class.getName());

    private T snapshot;
    private final Consumer<SnapshotHolder<T>> closeCallback;
    private final long snapshotId;
    private final Function<T, T> refreshProcedure;

    public SnapshotHolder(T snapshot,
                          long snapshotId,
                          Consumer<SnapshotHolder<T>> closeCallback,
                          Function<T, T> refreshProcedure) {
        this.snapshot = snapshot;
        this.snapshotId = snapshotId;
        this.closeCallback = closeCallback;
        this.refreshProcedure = refreshProcedure;
    }

    public T getSnapshot() {
        return snapshot;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String toString() {
        return "SnapshotHolder{" +
            "snapshot=" + snapshot +
            ", snapshotId=" + snapshotId +
            '}';
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("Refreshing the snapshot and calling the close callback...");
        this.snapshot = refreshProcedure.apply(this.snapshot);
        closeCallback.accept(this);
    }
}
