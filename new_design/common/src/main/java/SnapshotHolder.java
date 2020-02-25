public class SnapshotHolder<T extends AutoCloseable> implements AutoCloseable{
    private T snapshot;

    public SnapshotHolder(T snapshot) {
        this.snapshot = snapshot;
    }

    public T getSnapshot() {
        return snapshot;
    }

    @Override
    public void close() throws Exception {
        snapshot.close();
    }
}
