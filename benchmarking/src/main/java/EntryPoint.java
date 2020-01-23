import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class EntryPoint {
    private static final Logger logger = Logger.getLogger(EntryPoint.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        logger.info("Benchmarking: " + Constants.random);

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
            psqlConsumer.listenBlockingly();
        });
        executorService.submit(() -> {
            LoopingConsumer<Long, StupidStreamObject> luceneConsumer =
                new LoopingConsumer<>(LuceneUtils.getConsumer(luceneInitArgs));
            luceneConsumer.subscribe(luceneStorageSystem);
            luceneConsumer.listenBlockingly();
        });
        Thread.sleep(1000);

        for (int i=0; i<100; i++) {
            storageAPI.postMessage(new Message("simeon", "Hi for the " + i + "th time"));
            storageAPI.allMessages();
        }

        System.out.println(storageAPI.searchAndDetails("99th"));
    }
}
