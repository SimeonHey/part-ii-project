import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class EntryPoint {
    private static final Logger logger = Logger.getLogger(EntryPoint.class.getName());

    private static final int NUM_MESSAGES = 500;

    private static final AtomicInteger psqlCounter = new AtomicInteger(0);
    private static final AtomicInteger luceneCounter = new AtomicInteger(0);

    static class Pair {
        public int first, second;

        public Pair(int first, int second) {
            this.first = first;
            this.second = second;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        ExecutorService executorService = Executors.newFixedThreadPool(2 + NUM_MESSAGES);
        ArrayBlockingQueue<Pair> cntQueue = new ArrayBlockingQueue<>(2000);

        logger.info("Benchmarking: " + Constants.PROJECT_NAME);

        StorageAPI storageAPI = StorageAPIUtils.initFromArgs(new StorageAPIUtils.StorageAPIInitArgs(
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_PORT
        ));

        PsqlUtils.PsqlInitArgs psqlInitArgs = new PsqlUtils.PsqlInitArgs(
            Constants.PSQL_ADDRESS,
            Constants.PSQL_USER_PASS,
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS,
            Constants.PSQL_LISTEN_PORT
        );
        PsqlStorageSystem psqlStorageSystem = PsqlUtils.getStorageSystem(psqlInitArgs);

        LuceneUtils.LuceneInitArgs luceneInitArgs = new LuceneUtils.LuceneInitArgs(
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS,
            Constants.LUCENE_PSQL_CONTACT_ENDPOINT
        );
        LuceneStorageSystem luceneStorageSystem = LuceneUtils.getStorageSystem(luceneInitArgs);

        executorService.submit(() -> {
            LoopingConsumer<Long, StupidStreamObject> psqlConsumer =
                new LoopingConsumer<>(PsqlUtils.getConsumer(psqlInitArgs));
            psqlConsumer.subscribe(psqlStorageSystem);

            psqlConsumer.subscribe((consumerRecord) -> {
                cntQueue.add(new Pair(psqlCounter.incrementAndGet(), luceneCounter.get()));
            });

            psqlConsumer.listenBlockingly();
        });
        executorService.submit(() -> {
            LoopingConsumer<Long, StupidStreamObject> luceneConsumer =
                new LoopingConsumer<>(LuceneUtils.getConsumer(luceneInitArgs));
            luceneConsumer.subscribe(luceneStorageSystem);

            luceneConsumer.subscribe((consumerRecord) -> {
                cntQueue.add(new Pair(psqlCounter.get(), luceneCounter.incrementAndGet()));
            });

            luceneConsumer.listenBlockingly();
        });

        Thread.sleep(1000);

        long beginTime = System.nanoTime();

        List<Future> futures = new ArrayList<>();
        for (int i=0; i<NUM_MESSAGES; i++) {
            storageAPI.postMessage(new Message("simeon", "Message " + i));

            int finalI = i;
            Future thisFuture = executorService.submit(() -> {
                try {
                    long uuid = storageAPI.searchAndDetails(String.valueOf(finalI)).getUuid();
                    if (uuid == -1) {
                        throw new RuntimeException("MY ERROR: Couldnt find a message I should have been able to");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            futures.add(thisFuture);

//            storageAPI.allMessages();
        }

        // Wait for all the futures
        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        long elapsedTime = System.nanoTime() - beginTime;

        while (cntQueue.size() > 0) {
            Pair current = cntQueue.take();
            System.out.println("PSQL AND LUCENE SCORE - " + current.first + " : " + current.second);
        }

        System.out.println("Total time: " + elapsedTime / 1000000.0 + " ms");
    }
}
