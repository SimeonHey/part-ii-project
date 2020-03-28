import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class MultithreadedEventQueueExecutor implements Closeable {
    private final static Logger LOGGER = Logger.getLogger(MultithreadedEventQueueExecutor.class.getName());

    public interface Scheduler {
        void submitOperation(int identifier, Runnable runnable) throws InterruptedException;
        Runnable takeOperation(int identifier) throws InterruptedException;
    }

    public static class FifoScheduler implements Scheduler {
        private final LinkedBlockingQueue<Runnable> operations = new LinkedBlockingQueue<>();

        @Override
        public void submitOperation(int operationIdentifier, Runnable operation) throws InterruptedException {
            operations.put(operation);
        }

        @Override
        public Runnable takeOperation(int threadIdentifier) throws InterruptedException {
            return operations.take();
        }
    }

    public static class StaticChannelsScheduler implements Scheduler {
        private final ArrayList<LinkedBlockingQueue<Runnable>> operationsChannels;
        private final int numberOfChannels;

        public StaticChannelsScheduler(int numberOfOperationsChannels) {
            this.numberOfChannels = numberOfOperationsChannels;

            this.operationsChannels = new ArrayList<>(numberOfOperationsChannels);
            for (int i=0; i<numberOfOperationsChannels; i++) {
                operationsChannels.add(new LinkedBlockingQueue<>());
            }
        }

        @Override
        public void submitOperation(int identifier, Runnable runnable) throws InterruptedException {
            operationsChannels.get(identifier).put(runnable);
        }

        @Override
        public Runnable takeOperation(int identifier) throws InterruptedException {
            int channel = identifier % numberOfChannels;
            Runnable runnable = operationsChannels.get(channel).take();
            LOGGER.info("Thread with identifier " + identifier + " received an operation from channel " + channel);
            return runnable;
        }
    }

    private final List<Thread> runningThreads;
    private final Scheduler scheduler;
    private final int numberOfThreads;

    private boolean haltExecution;
    private AtomicInteger outstandingOperations = new AtomicInteger();

    private Thread getALoopingThread(int identifier) {
        return new Thread(() -> {
            while (!haltExecution) {
                try {
                    this.scheduler.takeOperation(identifier).run();
                    outstandingOperations.decrementAndGet();
                } catch (InterruptedException e) {
                    haltExecution = true;
                    LOGGER.warning("Error when trying to obtain next runnable operation: " + e);
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public MultithreadedEventQueueExecutor(int numberOfThreads, Scheduler scheduler) {
        this.numberOfThreads = numberOfThreads;
        this.scheduler = scheduler;

        this.runningThreads = new ArrayList<>(numberOfThreads);
        for (int i=0; i<numberOfThreads; i++) {
            this.runningThreads.add(this.getALoopingThread(i));
        }
        this.runningThreads.forEach(Thread::start);
    }

    public void submitOperation(int operationIdentifier, Runnable runnable) {
        operationIdentifier %= this.numberOfThreads; // TODO: not the cleanest thing

        try {
            outstandingOperations.incrementAndGet();
            scheduler.submitOperation(operationIdentifier, runnable);
        } catch (InterruptedException e) {
            LOGGER.warning("Error when trying to add a runnable operation");
            throw new RuntimeException(e);
        }
        LOGGER.info("Submitted an operation with identifier " + operationIdentifier);
    }

    public void submitOperation(Runnable runnable) {
        submitOperation(0, runnable);
    }

    public int getOutstandingOperations() {
        return outstandingOperations.get();
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
