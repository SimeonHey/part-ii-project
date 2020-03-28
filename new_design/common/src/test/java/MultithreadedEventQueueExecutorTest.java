import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class MultithreadedEventQueueExecutorTest {
    @Test
    public void testExecution() throws InterruptedException {
        MultithreadedEventQueueExecutor executor = new MultithreadedEventQueueExecutor(12);

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

        for (int i=0; i<arraySize; i++) {
            assertEquals(String.format("Error on index %d", i), numOps, atomicInts.get(i).get());
        }
    }
}