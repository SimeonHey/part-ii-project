import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

public class SnapshotHolder<T> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(SnapshotHolder.class.getName());

    private T snapshot;
    private final Consumer<SnapshotHolder<T>> closeCallback;
    private final long snapshotId;

    public SnapshotHolder(T snapshot,
                          long snapshotId,
                          Consumer<SnapshotHolder<T>> closeCallback) {
        this.snapshot = snapshot;
        this.snapshotId = snapshotId;
        this.closeCallback = closeCallback;
    }

    public SnapshotHolder<T> refresh(Function<T, T> refresher) {
        this.snapshot = refresher.apply(this.snapshot);
        return this;
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
        closeCallback.accept(this);
    }
}
