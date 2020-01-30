import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.logging.Logger;

public abstract class AutoSnapshottedEventStorageSystem<T extends AutoCloseable> extends EventStorageSystem {
    private final static Logger LOGGER = Logger.getLogger(AutoSnapshottedEventStorageSystem.class.getName());

    private final Semaphore connectionsSemaphore;
    private final ExecutorService readExecutorService;

    public AutoSnapshottedEventStorageSystem(String storageSystemName, String serverAddress, int maxReaders) {
        super(storageSystemName, serverAddress);

        this.connectionsSemaphore = maxReaders > 0 ? new Semaphore(maxReaders) : new Semaphore(1);
        this.readExecutorService = maxReaders > 0 ? Executors.newFixedThreadPool(maxReaders) : null;
    }

    private void executeReadOperation(Consumer<SnapshotHolder<T>> operation) {
        // Get the snapshot in the current thread, but make sure you have enough
        LOGGER.info(this.storageSystemName +
            " trying to execute a read operation; acquiring a semaphore resource (around " +
            connectionsSemaphore.availablePermits() + " are available)..");

        try {
            connectionsSemaphore.acquire();
        } catch (InterruptedException e) {
            LOGGER.warning("Error when acquiring a semaphore resource: " + e);
            throw new RuntimeException(e);
        }
        SnapshotHolder<T> snapshotHolder = getReadSnapshot();
        LOGGER.info(this.storageSystemName + " successfully got snapshot");

        // Use it in the new thread
        Runnable fullOperation = () -> {
            // Perform the operation
            operation.accept(snapshotHolder);

            // And close the snapshot 'connection'
            try {
                LOGGER.info(this.storageSystemName + " closing connection, and releasing a semaphore resource...");
                snapshotHolder.close();
                connectionsSemaphore.release();
            } catch (Exception e) {
                LOGGER.warning("Error when trying to close to snapshot holder");
                throw new RuntimeException(e);
            }
        };

        if (readExecutorService == null) {
            fullOperation.run();
        } else {
            readExecutorService.submit(fullOperation);
        }
    }

    @Override
    public void searchAndDetails(RequestSearchAndDetails requestSearchAndDetails) {
        this.executeReadOperation((snapshotHolder) -> this.searchAndDetails(snapshotHolder, requestSearchAndDetails));
    }

    @Override
    public void getMessageDetails(RequestMessageDetails requestMessageDetails) {
        this.executeReadOperation((snapshotHolder) -> this.getMessageDetails(snapshotHolder, requestMessageDetails));
    }

    @Override
    public void getAllMessages(RequestAllMessages requestAllMessages) {
        this.executeReadOperation((snapshotHolder) -> this.getAllMessages(snapshotHolder, requestAllMessages));
    }

    @Override
    public void searchMessage(RequestSearchMessage requestSearchMessage) {
        this.executeReadOperation((snapshotHolder) -> this.searchMessage(snapshotHolder, requestSearchMessage));
    }

    // Returns a read snapshot of the data at this precise moment
    protected abstract SnapshotHolder<T> getReadSnapshot();

    // Read requests
    protected abstract void searchAndDetails(SnapshotHolder<T> snapshotHolder,
                                          RequestSearchAndDetails requestSearchAndDetails);
    protected abstract void getMessageDetails(SnapshotHolder<T> snapshotHolder,
                                           RequestMessageDetails requestMessageDetails);
    protected abstract void getAllMessages(SnapshotHolder<T> snapshotHolder,
                                        RequestAllMessages requestAllMessages);
    protected abstract void searchMessage(SnapshotHolder<T> snapshotHolder,
                                       RequestSearchMessage requestSearchMessage);
}
