import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public abstract class SnapshottedDatabase<S> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(SnapshottedDatabase.class.getName());

    private final Semaphore snapshotsSemaphore;
    private final BlockingQueue<SnapshotHolder<S>> concurrentConectionsPool;

    protected SnapshottedDatabase(int maxSnapshots) {
        this.concurrentConectionsPool = new LinkedBlockingDeque<>(maxSnapshots);
        this.snapshotsSemaphore = new Semaphore(maxSnapshots);
    }

    abstract S getMainDataView();
    abstract S freshConcurrentSnapshot();
    abstract S refreshSnapshot(S bareSnapshot); // TODO: I assume that this doesn't take up a snapshot resource, but
    // it does with Lucene

    public final SnapshotHolder<S> getConcurrentSnapshot() {
        // First, try and re-use one from the poll, with no blocking!
        SnapshotHolder<S> pooled = concurrentConectionsPool.poll(); // TODO: Might want to wait for a while :)
        if (pooled != null) {
            LOGGER.info("Refreshing and returning a pooled snapshot");
            return pooled.refresh(this::refreshSnapshot);
        }

        // If such is not available, try and create a new one, no blocking again (because nothing is releasing
        // resources)
        boolean canAcquire = snapshotsSemaphore.tryAcquire();
        if (canAcquire) {
            LOGGER.info("Returning a fresh snapshot");
            return new SnapshotHolder<>(
                freshConcurrentSnapshot(),
                0L, // TODO
                this::concurrentSnapshotClosedCallback);
        }

        // If we can't create a new one because the limit has been reached, block until a pooled one is available
        try {
            LOGGER.info("Waiting for a pooled snapshot and returning it...");
            return concurrentConectionsPool.take();
        } catch (InterruptedException e) {
            LOGGER.warning("Error when waiting on a pooled snapshot to become available: " + e);
            throw new RuntimeException(e);
        }
    }

    private void concurrentSnapshotClosedCallback(SnapshotHolder<S> snapshot) {
        LOGGER.info("Pooling snapshot " + snapshot);

        try {
            concurrentConectionsPool.put(snapshot);
        } catch (InterruptedException e) {
            LOGGER.warning("Error when trying to put a wrapped snapshot back in the pool to be reused: " + e);
            throw new RuntimeException(e);
        }
    }
}
