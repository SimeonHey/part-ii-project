import java.io.IOException;
import java.sql.SQLException;

class Utils {
    private static Trinity savedInstanceBasic;
    private static ManualTrinity savedInstanceManual;

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
            Thread.sleep(1000);
        }
    }

    static class ManualTrinity extends Trinity {
        public final ManualConsumer<Long, StupidStreamObject> manualConsumerLucene;
        public final ManualConsumer<Long, StupidStreamObject> manualConsumerPsql;

        ManualTrinity(PsqlStorageSystem psqlStorageSystem,
                      LuceneStorageSystem luceneStorageSystem,
                      StorageAPI storageAPI,
                      ManualConsumer<Long, StupidStreamObject> manualConsumerLucene,
                      ManualConsumer<Long, StupidStreamObject> manualConsumerPsql) {
            super(psqlStorageSystem, luceneStorageSystem, storageAPI);
            this.manualConsumerLucene = manualConsumerLucene;
            this.manualConsumerPsql = manualConsumerPsql;
        }

        public int progressLucene() {
            return manualConsumerLucene.consumeAvailableRecords();
        }

        public int progressPsql() {
            return manualConsumerPsql.consumeAvailableRecords();
        }
    }

    static Trinity basicInitialization() throws IOException, SQLException, InterruptedException {
        if (savedInstanceBasic != null) {
            return savedInstanceBasic;
        }

        PsqlUtils.PsqlInitArgs psqlInitArgs = new PsqlUtils.PsqlInitArgs(
            Constants.PSQL_ADDRESS,
            Constants.PSQL_USER_PASS,
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS,
            Constants.PSQL_LISTEN_PORT);

        PsqlStorageSystem psqlStorageSystem = PsqlUtils.getStorageSystem(psqlInitArgs);
        LoopingConsumer<Long, StupidStreamObject> loopingConsumerPsql =
            new LoopingConsumer<>(PsqlUtils.getConsumer(psqlInitArgs));
        loopingConsumerPsql.subscribe(psqlStorageSystem);

        LuceneUtils.LuceneInitArgs luceneInitArgs = new LuceneUtils.LuceneInitArgs(
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS,
            Constants.LUCENE_PSQL_CONTACT_ENDPOINT);

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

        Thread.sleep(1000);

        savedInstanceBasic = new Trinity(psqlStorageSystem, luceneStorageSystem, storageAPI);
        return savedInstanceBasic;
    }

    static ManualTrinity manualConsumerInitialization() throws IOException {
        if (savedInstanceManual != null) {
            return savedInstanceManual;
        }

        PsqlUtils.PsqlInitArgs psqlInitArgs = new PsqlUtils.PsqlInitArgs(
            Constants.PSQL_ADDRESS,
            Constants.PSQL_USER_PASS,
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS,
            Constants.PSQL_LISTEN_PORT);

        PsqlStorageSystem psqlStorageSystem = PsqlUtils.getStorageSystem(psqlInitArgs);
        ManualConsumer<Long, StupidStreamObject> manualConsumerPsql =
            new ManualConsumer<>(new DummyConsumer("PSQL"));
        manualConsumerPsql.subscribe(psqlStorageSystem);

        LuceneUtils.LuceneInitArgs luceneInitArgs = new LuceneUtils.LuceneInitArgs(
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS,
            Constants.LUCENE_PSQL_CONTACT_ENDPOINT);

        LuceneStorageSystem luceneStorageSystem = LuceneUtils.getStorageSystem(luceneInitArgs);
        ManualConsumer<Long, StupidStreamObject> manualConsumerLucene =
            new ManualConsumer<>(new DummyConsumer("Lucene"));
        manualConsumerLucene.subscribe(luceneStorageSystem);

        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = new StorageAPIUtils.StorageAPIInitArgs(
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_PORT);
        StorageAPI storageAPI = StorageAPIUtils.initFromArgsWithDummyKafka(storageAPIInitArgs);

        manualConsumerPsql.moveAllToLatest();
        manualConsumerLucene.moveAllToLatest();

        savedInstanceManual = new ManualTrinity(psqlStorageSystem, luceneStorageSystem, storageAPI,
            manualConsumerLucene, manualConsumerPsql);

        return savedInstanceManual;
    }

    static void letThatSinkIn(Runnable r) throws InterruptedException {
        r.run();
        Thread.sleep(Constants.KAFKA_CONSUME_DELAY_MS * 2);
    }
}
