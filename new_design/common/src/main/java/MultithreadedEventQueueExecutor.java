import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class MultithreadedEventQueueExecutor implements Closeable {
    private final static Logger LOGGER = Logger.getLogger(MultithreadedEventQueueExecutor.class.getName());

    private final LinkedBlockingQueue<Runnable> operations;
    private final List<Thread> runningThreads;

    private boolean haltExecution;
    private int outstandingOperations;

    private Thread getLoopingThread() {
        return new Thread(() -> {
            while (!haltExecution) {
                try {
                    operations.take().run();
                    outstandingOperations -= 1;
                } catch (InterruptedException e) {
                    haltExecution = true;
                    LOGGER.warning("Error when trying to obtain next runnable operation: " + e);
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public MultithreadedEventQueueExecutor(int numberOfThreads) {
        this.operations = new LinkedBlockingQueue<>();
        this.runningThreads = new ArrayList<>(numberOfThreads);

        for (int i=0; i<numberOfThreads; i++) {
            this.runningThreads.add(this.getLoopingThread());
        }

        this.runningThreads.forEach(Thread::start);
    }

    public void submitOperation(Runnable runnable) {
        try {
            outstandingOperations += 1;
            operations.put(runnable);
        } catch (InterruptedException e) {
            LOGGER.warning("Error when trying to add a runnable operation");
            throw new RuntimeException(e);
        }
    }

    public int getOutstandingOperations() {
        return outstandingOperations;
    }

    @Override
    public void close() {
        this.haltExecution = true;
        for (Thread runningThread : this.runningThreads) {
            try {
                runningThread.join();
            } catch (InterruptedException e) {
                LOGGER.warning("Error when waiting for a running thread to stop executing");
                throw new RuntimeException(e);
            }
        }
    }
}
