import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

class MultithreadedEventQueueExecutor implements Closeable {
    private final static Logger LOGGER = Logger.getLogger(MultithreadedEventQueueExecutor.class.getName());

    interface Scheduler {
        void submitOperation(int operationIdentifier, Runnable runnable) throws InterruptedException;
        Runnable takeOperation(int threadIdentifier) throws InterruptedException;
    }

    static class FifoScheduler implements Scheduler {
        private final LinkedBlockingQueue<Runnable> operations = new LinkedBlockingQueue<>();

        @Override
        public void submitOperation(int operationIdentifier, Runnable operation) throws InterruptedException {
            LOGGER.info("Operation with identifier " + operationIdentifier + " was submitted");
            operations.put(operation);
        }

        @Override
        public Runnable takeOperation(int threadIdentifier) throws InterruptedException {
            Runnable operation = operations.take();
            LOGGER.info("Thread with identifier " + threadIdentifier + " received an operation");
            return operation;
        }
    }

    static class StaticChannelsScheduler implements Scheduler {
        private final ArrayList<LinkedBlockingQueue<Runnable>> operationsChannels;
        private final int numberOfChannels;

        StaticChannelsScheduler(int numberOfOperationsChannels) {
            this.numberOfChannels = numberOfOperationsChannels;

            this.operationsChannels = new ArrayList<>(numberOfOperationsChannels);
            for (int i=0; i<numberOfOperationsChannels; i++) {
                operationsChannels.add(new LinkedBlockingQueue<>());
            }
        }

        @Override
        public void submitOperation(int operationIdentifier, Runnable runnable) throws InterruptedException {
            int channel = operationIdentifier % numberOfChannels;
            LOGGER.info("Submitting operation with identifier " + operationIdentifier +
                " in channel " + channel);

            operationsChannels.get(operationIdentifier).put(runnable);
        }

        @Override
        public Runnable takeOperation(int threadIdentifier) throws InterruptedException {
            int channel = threadIdentifier % numberOfChannels;

            Runnable runnable = operationsChannels.get(channel).take();
            LOGGER.info("Thread with identifier " + threadIdentifier +
                " receives an operation from channel " + channel);
            return runnable;
        }
    }

    private final List<Thread> runningThreads;
    private final Scheduler scheduler;
    private final int numberOfThreads;

    private boolean haltExecution;
    private AtomicInteger outstandingOperations = new AtomicInteger();

    private Thread getALoopingThread(int threadIdentifier) {
        return new Thread(() -> {
            while (!haltExecution) {
                try {
                    this.scheduler.takeOperation(threadIdentifier).run();
                    outstandingOperations.decrementAndGet();
                } catch (InterruptedException e) {
                    haltExecution = true;
                    LOGGER.warning("Error when trying to obtain next runnable operation: " + e);
                    throw new RuntimeException(e);
                }
            }
        });
    }

    MultithreadedEventQueueExecutor(int numberOfThreads, Scheduler scheduler) {
        this.numberOfThreads = numberOfThreads;
        this.scheduler = scheduler;

        this.runningThreads = new ArrayList<>(numberOfThreads);
        for (int i=0; i<numberOfThreads; i++) {
            this.runningThreads.add(this.getALoopingThread(i));
        }
        this.runningThreads.forEach(Thread::start);
    }

    void submitOperation(int operationIdentifier, Runnable runnable) {
        try {
            outstandingOperations.incrementAndGet();
            scheduler.submitOperation(operationIdentifier, runnable);
        } catch (InterruptedException e) {
            LOGGER.warning("Error when trying to add a runnable operation");
            throw new RuntimeException(e);
        }
        LOGGER.info("Submitted an operation with identifier " + operationIdentifier);
    }

    void submitOperation(Runnable runnable) {
        submitOperation(0, runnable);
    }

    int getOutstandingOperations() {
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

    @Override
    public String toString() {
        return "MultithreadedEventQueueExecutor{" +
            "scheduler=" + scheduler +
            ", numberOfThreads=" + numberOfThreads +
            ", haltExecution=" + haltExecution +
            ", outstandingOperations=" + outstandingOperations +
            '}';
    }
}
