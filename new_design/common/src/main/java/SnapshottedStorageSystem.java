import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public abstract class SnapshottedStorageSystem<S> implements AutoCloseable {
    private final static Logger LOGGER = Logger.getLogger(SnapshottedStorageSystem.class.getName());

    private final Semaphore connectionsSemaphore;
    private final BlockingQueue<SnapshotHolder<S>> concurrentConectionsPool;

    protected SnapshottedStorageSystem(int maxConnections) {
        this.concurrentConectionsPool = new LinkedBlockingDeque<>(maxConnections);
        this.connectionsSemaphore = new Semaphore(maxConnections);
    }

    abstract S getDefaultSnapshot();
    abstract S freshConcurrentSnapshot();
    abstract S refreshSnapshot(S bareSnapshot); // TODO: I assume that this doesn't take up a snapshot resource, but
    // it does with Lucene

    public final SnapshotHolder<S> getConcurrentSnapshot() {
        // First, try and re-use one from the poll, with no blocking!
        SnapshotHolder<S> pooled = concurrentConectionsPool.poll(); // TODO: Might want to wait for a while :)
        if (pooled != null) {
            LOGGER.info("Returning a pooled connection");
            return pooled;
        }

        // If such is not available, try and create a new one, no blocking again (because nothing is releasing
        // resources)
        boolean canAcquire = connectionsSemaphore.tryAcquire();
        if (canAcquire) {
            LOGGER.info("Returning a fresh connection");
            return new SnapshotHolder<>(
                freshConcurrentSnapshot(),
                0L, // TODO
                this::concurrentConnectionClosedCallback,
                this::refreshSnapshot);
        }

        // If we can't create a new one because the limit has been reached, block until a pooled one is available
        try {
            LOGGER.info("Waiting for a pooled connection and returning it...");
            return concurrentConectionsPool.take();
        } catch (InterruptedException e) {
            LOGGER.warning("Error when waiting on a pooled connection to become available: " + e);
            throw new RuntimeException(e);
        }
    }

    private void concurrentConnectionClosedCallback(SnapshotHolder<S> snapshot) {
        LOGGER.info("Putting connection " + snapshot + " back into the pool...");

        try {
            concurrentConectionsPool.put(snapshot);
        } catch (InterruptedException e) {
            LOGGER.warning("Error when trying to put a wrapped connection back in the pool to be reused: " + e);
            throw new RuntimeException(e);
        }
    }
}
