import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

class Utils {
    private static final Logger LOGGER = Logger.getLogger(Utils.class.getName());

    private static ManualTrinity savedInstanceManual;
    private static Trinity savedInstance;

    static class Trinity implements AutoCloseable{
        public final JointStorageSystem<Connection> psqlSimpleOlep;
        public final LuceneStorageSystem luceneStorageSystem;
        public final StorageAPI storageAPI;

        Trinity(JointStorageSystem<Connection> psqlSimpleOlep, LuceneStorageSystem luceneStorageSystem,
                StorageAPI storageAPI) {
            this.psqlSimpleOlep = psqlSimpleOlep;
            this.luceneStorageSystem = luceneStorageSystem;
            this.storageAPI = storageAPI;
        }

        @Override
        public void close() throws Exception {
            this.storageAPI.deleteAllMessages(); // Produces a Kafka message
            Thread.sleep(1000);

            /*this.storageAPI.close();
            this.psqlStorageSystem.close();
            this.luceneStorageSystem.close();
            Thread.sleep(1000);*/
        }
    }

    static class ManualTrinity extends Trinity {
        public final ManualConsumer<Long, StupidStreamObject> manualConsumerLucene;
        public final ManualConsumer<Long, StupidStreamObject> manualConsumerPsql;

        ManualTrinity(JointStorageSystem<Connection> psqlConcurrentSnapshots,
                      LuceneStorageSystem luceneStorageSystem,
                      StorageAPI storageAPI,
                      ManualConsumer<Long, StupidStreamObject> manualConsumerLucene,
                      ManualConsumer<Long, StupidStreamObject> manualConsumerPsql) {
            super(psqlConcurrentSnapshots, luceneStorageSystem, storageAPI);
            this.manualConsumerLucene = manualConsumerLucene;
            this.manualConsumerPsql = manualConsumerPsql;
        }

        public int progressLucene() {
            return manualConsumerLucene.consumeAvailableRecords();
        }

        public int progressPsql() {
            return manualConsumerPsql.consumeAvailableRecords();
        }

        @Override
        public void close() throws Exception {
            /*this.storageAPI.deleteAllMessages();

            this.progressLucene();
            this.progressPsql();

            Thread.sleep(1000); // TODO: Could do with a hook in the producers*/

            this.manualConsumerPsql.close();
            this.manualConsumerLucene.close();

            this.storageAPI.close();
            this.psqlSimpleOlep.close();
            this.luceneStorageSystem.close();
        }
    }

    static Trinity basicInitialization() throws IOException, InterruptedException {
        if (savedInstance != null) {
            return savedInstance;
        }

        PsqlUtils.PsqlInitArgs psqlInitArgs = PsqlUtils.PsqlInitArgs.defaultValues();

        JointStorageSystem<Connection> psqlConcurrentSnapshots =
            PsqlStorageSystemsFactory.simpleOlep(Executors.newFixedThreadPool(1));
        /*LoopingConsumer<Long, StupidStreamObject> loopingConsumerPsql =
            new LoopingConsumer<>(PsqlUtils.getConsumer(psqlInitArgs));
        loopingConsumerPsql.subscribe(psqlConcurrentSnapshots);*/

        LuceneUtils.LuceneInitArgs luceneInitArgs = LuceneUtils.LuceneInitArgs.defaultValues();

        LuceneStorageSystem luceneStorageSystem = LuceneUtils.getStorageSystem(luceneInitArgs);
        LoopingConsumer<Long, StupidStreamObject> loopingConsumerLucene =
            new LoopingConsumer<>(LuceneUtils.getConsumer(luceneInitArgs));
        loopingConsumerLucene.subscribe(luceneStorageSystem);

        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = StorageAPIUtils.StorageAPIInitArgs.defaultValues();
        StorageAPI storageAPI = StorageAPIUtils.initFromArgs(storageAPIInitArgs);

        /*loopingConsumerPsql.moveAllToLatest();
        loopingConsumerLucene.moveAllToLatest();

        new Thread(loopingConsumerPsql::listenBlockingly).start();
        new Thread(loopingConsumerLucene::listenBlockingly).start();*/

        Thread.sleep(1000);

        savedInstance = new Trinity(psqlConcurrentSnapshots, luceneStorageSystem, storageAPI);
        return savedInstance;
    }

    static ManualTrinity manualConsumerInitialization(int readerThreads) throws IOException {
        if (5 == 5 ) {
            throw new RuntimeException("No.");
        }
        /*if (savedInstanceManual != null) {
            return savedInstanceManual;
        }*/

        PsqlUtils.PsqlInitArgs psqlInitArgs = PsqlUtils.PsqlInitArgs.customValues(
            Constants.PSQL_ADDRESS,
            Constants.PSQL_USER_PASS,
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS_ALT,
            Constants.PSQL_LISTEN_PORT_ALT,
            readerThreads);

        JointStorageSystem<Connection> psqlConcurrentSnapshots =
            PsqlStorageSystemsFactory.simpleOlep(Executors.newFixedThreadPool(1));
        ManualConsumer<Long, StupidStreamObject> manualConsumerPsql =
            new ManualConsumer<>(new DummyConsumer("PSQL"));
//        manualConsumerPsql.subscribe(psqlConcurrentSnapshots);

        LuceneUtils.LuceneInitArgs luceneInitArgs = LuceneUtils.LuceneInitArgs.fromValues(
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_ADDRESS_ALT,
            Constants.LUCENE_PSQL_CONTACT_ENDPOINT_ALT,
            readerThreads);

        LuceneStorageSystem luceneStorageSystem = LuceneUtils.getStorageSystem(luceneInitArgs);
        ManualConsumer<Long, StupidStreamObject> manualConsumerLucene =
            new ManualConsumer<>(new DummyConsumer("Lucene"));
        manualConsumerLucene.subscribe(luceneStorageSystem);

        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = StorageAPIUtils.StorageAPIInitArgs.customValues(
            Constants.KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_PORT_ALT);
        StorageAPI storageAPI = StorageAPIUtils.initFromArgsWithDummyKafka(storageAPIInitArgs);

        manualConsumerPsql.moveAllToLatest();
        manualConsumerLucene.moveAllToLatest();

        savedInstanceManual = new ManualTrinity(psqlConcurrentSnapshots, luceneStorageSystem, storageAPI,
            manualConsumerLucene, manualConsumerPsql);

        return savedInstanceManual;
    }

    static void letThatSinkIn(Runnable r) throws InterruptedException {
        r.run();
        Thread.sleep(Constants.KAFKA_CONSUME_DELAY_MS * 2);
    }

    static void letThatSinkInManually(ManualTrinity manualTrinity, Runnable r) {
        r.run();
        int cntPsql = manualTrinity.progressPsql();
        int cntLucene = manualTrinity.progressLucene();

        LOGGER.info(String.format("Manually consumed %d PSQL and %d Lucene messages", cntPsql, cntLucene));
    }
}
