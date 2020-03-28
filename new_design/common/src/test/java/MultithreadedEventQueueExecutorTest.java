import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class MultithreadedEventQueueExecutorTest {
    @Test
    public void testConcurrentExecution() throws InterruptedException {
        int numberOfThreads = 12;

        MultithreadedEventQueueExecutor executor = new MultithreadedEventQueueExecutor(numberOfThreads,
            new MultithreadedEventQueueExecutor.FifoScheduler());

        // That's about 1 billion operations - it would be super slow on 1 thread
        int numOps = 1_000;
        int arraySize = 1_000_000;

        List<AtomicInteger> atomicInts = new ArrayList<>(arraySize);
        for (int i=0; i<arraySize; i++) {
            atomicInts.add(new AtomicInteger(0));
        }

        for (int i=0; i<numOps; i++) {
            executor.submitOperation(() -> {
                for (int j=0; j<arraySize; j++) {
                    atomicInts.get(j).incrementAndGet();
                }
            });
        }

        Thread.sleep(1000);
        int outstanding;
        while ((outstanding = executor.getOutstandingOperations()) > 0) {
            System.out.println("Waiting until " + outstanding + " operations complete");
            Thread.sleep(1000);
        }

        System.out.println("Checking the answers...");
        for (int i=0; i<arraySize; i++) {
            assertEquals(String.format("Error on index %d", i), numOps, atomicInts.get(i).get());
        }
    }

    @Test
    public void testChannelsScheduler() throws InterruptedException {
        int numberOfThreads = 6;
        int numberOfOperationsChannels = 5;

        MultithreadedEventQueueExecutor executor = new MultithreadedEventQueueExecutor(numberOfThreads,
            new MultithreadedEventQueueExecutor.StaticChannelsScheduler(
                numberOfOperationsChannels + 1));

        // Block the last channel completely
        int hoggingOperations = numberOfThreads * 100;
        for (int i=0; i<hoggingOperations; i++) {
            executor.submitOperation(numberOfOperationsChannels, () -> {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // That's about 1 billion operations - it would be super slow on 1 thread
        int numOps = 1_000;
        int arraySize = 1_000_000;

        List<AtomicInteger> atomicInts = new ArrayList<>(arraySize);
        for (int i=0; i<arraySize; i++) {
            atomicInts.add(new AtomicInteger(0));
        }

        for (int i=0; i<numOps; i++) {
            int channel = i % numberOfOperationsChannels;

            executor.submitOperation(channel, () -> {
                for (int j=0; j<arraySize; j++) {
                    atomicInts.get(j).incrementAndGet();
                }
            });
        }

        // Wait until all but the hogging operations are completed
        int outstanding;
        while ((outstanding = executor.getOutstandingOperations()) > hoggingOperations) {
            System.out.println("Waiting until " + (outstanding - hoggingOperations) + " operations complete");
            Thread.sleep(1000);
        }

        System.out.println("Checking the answers...");
        for (int i=0; i<arraySize; i++) {
            assertEquals(String.format("Error on index %d", i), numOps, atomicInts.get(i).get());
        }
    }
}