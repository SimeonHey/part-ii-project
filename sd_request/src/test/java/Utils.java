import java.io.IOException;
import java.sql.SQLException;

class Utils {
    private static Trinity savedInstance;

    static class Trinity implements AutoCloseable{
        public final PsqlStorageSystem psqlStorageSystem;
        public final LuceneStorageSystem luceneStorageSystem;
        public final StorageAPI storageAPI;

        Trinity(PsqlStorageSystem psqlStorageSystem, LuceneStorageSystem luceneStorageSystem, StorageAPI storageAPI) {
            this.psqlStorageSystem = psqlStorageSystem;
            this.luceneStorageSystem = luceneStorageSystem;
            this.storageAPI = storageAPI;
        }

        @Override
        public void close() throws Exception {
            this.storageAPI.deleteAllMessages(); // Produces a Kafka message
        }
    }

    static Trinity basicInitialization() throws IOException, SQLException {
        if (savedInstance != null) {
            return savedInstance;
        }

        PsqlUtils.PsqlInitArgs psqlInitArgs = new PsqlUtils.PsqlInitArgs(
            Constants.PSQL_ADDRESS,
            Constants.PSQL_USER_PASS,
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS);

        PsqlStorageSystem psqlStorageSystem = PsqlUtils.getStorageSystem(psqlInitArgs);
        LoopingConsumer<Long, StupidStreamObject> loopingConsumerPsql =
            new LoopingConsumer<>(PsqlUtils.getConsumer(psqlInitArgs));
        loopingConsumerPsql.subscribe(psqlStorageSystem);

        LuceneUtils.LuceneInitArgs luceneInitArgs = new LuceneUtils.LuceneInitArgs(
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS);

        LuceneStorageSystem luceneStorageSystem = LuceneUtils.getStorageSystem(luceneInitArgs);
        LoopingConsumer<Long, StupidStreamObject> loopingConsumerLucene =
            new LoopingConsumer<>(LuceneUtils.getConsumer(luceneInitArgs));
        loopingConsumerLucene.subscribe(luceneStorageSystem);

        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = new StorageAPIUtils.StorageAPIInitArgs(
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_PORT);
        StorageAPI storageAPI = StorageAPIUtils.initFromArgs(storageAPIInitArgs);

        loopingConsumerPsql.moveAllToLatest();
        loopingConsumerLucene.moveAllToLatest();
        new Thread(loopingConsumerPsql::listenBlockingly).start();
        new Thread(loopingConsumerLucene::listenBlockingly).start();

        savedInstance = new Trinity(psqlStorageSystem, luceneStorageSystem, storageAPI);
        return savedInstance;
    }

    static void letThatSinkIn(Runnable r) throws InterruptedException {
        r.run();
        Thread.sleep(Constants.KAFKA_CONSUME_DELAY_MS * 2);
    }
}
