import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
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

    private static final String OUTPUT_DIRECTORY = "/home/simeon/Documents/Cambridge/project/";

    private static final int CONCURRENT_READS = 100;
    private static final int NUM_MESSAGES = 500;

    private static final AtomicInteger psqlCounter = new AtomicInteger(0);
    private static final AtomicInteger luceneCounter = new AtomicInteger(0);

    private static final ExecutorService longRunningExecutorService =
        Executors.newFixedThreadPool(2);

    static class Pair {
        public int first, second;

        public Pair(int first, int second) {
            this.first = first;
            this.second = second;
        }
    }

    public static void writeToCsv(String outputDestination, List<List<String>> dataLines) {
        File csvOutputFile = new File(outputDestination);
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            dataLines.forEach(dataLine -> pw.println(String.join(",", dataLine)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void pairsToCsv(String fileName, ArrayBlockingQueue<Pair> pairs) {
        List<String> valuesPSQL = new ArrayList<>();
        List<String> valuesLucene = new ArrayList<>();

        while (pairs.size() > 0) {
            Pair pair = null;
            try {
                pair = pairs.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            valuesPSQL.add(String.valueOf(pair.first));
            valuesLucene.add(String.valueOf(pair.second));
        }

        writeToCsv(OUTPUT_DIRECTORY + fileName, List.of(valuesPSQL, valuesLucene));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ArrayBlockingQueue<Pair> cntQueue = new ArrayBlockingQueue<>(2000);

        logger.info("Benchmarking: " + Constants.PROJECT_NAME);

        // Initialize storage systems
        PsqlUtils.PsqlInitArgs psqlInitArgs = PsqlUtils.PsqlInitArgs.defaultValues();
        psqlInitArgs.numberOfReaders = CONCURRENT_READS;
        PsqlStorageSystem psqlStorageSystem = PsqlUtils.getStorageSystem(psqlInitArgs);

        LuceneUtils.LuceneInitArgs luceneInitArgs = LuceneUtils.LuceneInitArgs.defaultValues();
        LuceneStorageSystem luceneStorageSystem = LuceneUtils.getStorageSystem(luceneInitArgs);

        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = StorageAPIUtils.StorageAPIInitArgs.defaultValues();
        StorageAPI storageAPI = StorageAPIUtils.initFromArgs(storageAPIInitArgs);

        // Initialize consumers
        longRunningExecutorService.submit(() -> {
            LoopingConsumer<Long, StupidStreamObject> psqlConsumer =
                new LoopingConsumer<>(PsqlUtils.getConsumer(psqlInitArgs));
            psqlConsumer.moveAllToLatest();

            psqlConsumer.subscribe(psqlStorageSystem);

            psqlConsumer.subscribe((consumerRecord) -> {
                cntQueue.add(new Pair(psqlCounter.incrementAndGet(), luceneCounter.get()));
            });

            psqlConsumer.listenBlockingly();
        });
        longRunningExecutorService.submit(() -> {
            LoopingConsumer<Long, StupidStreamObject> luceneConsumer =
                new LoopingConsumer<>(LuceneUtils.getConsumer(luceneInitArgs));
            luceneConsumer.moveAllToLatest();

            luceneConsumer.subscribe(luceneStorageSystem);

            luceneConsumer.subscribe((consumerRecord) -> {
                cntQueue.add(new Pair(psqlCounter.get(), luceneCounter.incrementAndGet()));
            });

            luceneConsumer.listenBlockingly();
        });
        Thread.sleep(1000);

        long beginTime = System.nanoTime();

        List<Future<ResponseMessageDetails>> futures = new ArrayList<>();
        for (int i=1; i<=NUM_MESSAGES; i++) {
            System.out.println("Posting message " + i + " out of " + NUM_MESSAGES);

            storageAPI.postMessage(new Message("simeon", "Message " + i));

            Future<ResponseMessageDetails> detailsFuture1 =
                storageAPI.searchAndDetailsFuture(String.valueOf(i));

            futures.add(detailsFuture1);
        }

        // Wait for all the futures & check results
        futures.forEach(future -> {
            try {
                long uuid = future.get().getUuid();

                if (uuid == -1) {
                    throw new RuntimeException("MY ERROR: Expected a good uuid, but got " + uuid);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        ResponseMessageDetails liastDetails =
            storageAPI.searchAndDetails("" + NUM_MESSAGES);
        System.out.println("Last details is: " + liastDetails);

        long elapsedTime = System.nanoTime() - beginTime;

        pairsToCsv("post.sd-" + CONCURRENT_READS + ".csv", cntQueue);

//        while (cntQueue.size() > 0) {
//            Pair current = cntQueue.take();
//            System.out.println("PSQL AND LUCENE SCORE - " + current.first + " : " + current.second);
//        }

        System.out.println("Total time: " + elapsedTime / 1000000.0 + " ms");
    }
}
