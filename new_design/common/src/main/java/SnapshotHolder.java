import java.util.function.Consumer;
import java.util.function.Function;

public class SnapshotHolder<T> implements AutoCloseable {
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
        this.snapshot = refreshProcedure.apply(this.snapshot);
        closeCallback.accept(this);
    }
}
