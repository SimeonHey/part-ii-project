import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

class Utils {
    private static final Logger LOGGER = Logger.getLogger(Utils.class.getName());

    private static ManualTrinity savedInstanceManual;
    private static Trinity savedInstance;

    static class Trinity implements AutoCloseable{
        public final JointStorageSystem psqlStorageSystem;
        public final JointStorageSystem luceneStorageSystem;
        public final StorageAPI storageAPI;

        Trinity(JointStorageSystem psqlStorageSystem, JointStorageSystem luceneStorageSystem,
                StorageAPI storageAPI) {
            this.psqlStorageSystem = psqlStorageSystem;
            this.luceneStorageSystem = luceneStorageSystem;
            this.storageAPI = storageAPI;
        }

        @Override
        public void close() throws Exception {
            LOGGER.info("STARTING SHUTDOWN PROCEDURE");

            this.storageAPI.handleRequest(new RequestDeleteAllMessages()); // Produces a Kafka message
            this.storageAPI.waitForAllConfirmations();
            Thread.sleep(1000);

            /*this.storageAPI.close();
            this.psqlStorageSystem.close();
            this.luceneStorageSystem.close();
            Thread.sleep(1000);*/
        }
    }

    static class ManualTrinity extends Trinity {
        public final ManualConsumer<Long, StupidStreamObject> manualConsumerPsql;
        public final ManualConsumer<Long, StupidStreamObject> manualConsumerLucene;

        ManualTrinity(JointStorageSystem psqlConcurReads,
                      JointStorageSystem luceneConcurReads,
                      StorageAPI storageAPI,
                      ManualConsumer<Long, StupidStreamObject> manualConsumerPsql,
                      ManualConsumer<Long, StupidStreamObject> manualConsumerLucene) {
            super(psqlConcurReads, luceneConcurReads, storageAPI);
            this.manualConsumerPsql = manualConsumerPsql;
            this.manualConsumerLucene = manualConsumerLucene;
        }

        public int progressPsql() {
            return manualConsumerPsql.consumeAvailableRecords();
        }

        public int progressLucene() {
            return manualConsumerLucene.consumeAvailableRecords();
        }

        @Override
        public void close() throws Exception {
            LOGGER.info("STARTING SHUTDOWN PROCEDURE");
            /*this.storageAPI.deleteAllMessages();

            this.progressLucene();
            this.progressPsql();

            Thread.sleep(1000); // TODO: Could do with a hook in the producers*/

            this.manualConsumerPsql.close();
            this.manualConsumerLucene.close();

            this.storageAPI.close();
            this.psqlStorageSystem.close();
            this.luceneStorageSystem.close();
        }
    }

    static Trinity basicInitialization() throws IOException, InterruptedException {
        if (savedInstance != null) {
            LOGGER.info("Returning saved instance!");
            return savedInstance;
        }

        var psqlFactory = new PsqlStorageSystemsFactory(LoopingConsumer.fresh("psql",
            Constants.TEST_KAFKA_ADDRESS));
        var psqlConcurrentSnapshots = psqlFactory.concurSchedule();
        psqlFactory.listenBlockingly(Executors.newFixedThreadPool(1));

        var luceneFactory = new LuceneStorageSystemFactory(LoopingConsumer.fresh("lucene",
            Constants.TEST_KAFKA_ADDRESS), Constants.TEST_LUCENE_PSQL_CONTACT_ENDPOINT);
        JointStorageSystem luceneStorageSystem = luceneFactory.concurSchedule();
        luceneFactory.listenBlockingly(Executors.newFixedThreadPool(1));

        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = StorageAPIUtils.StorageAPIInitArgs.defaultTestValues();
        StorageAPI storageAPI = StorageAPIUtils.initFromArgsForTests(storageAPIInitArgs);

        Thread.sleep(1000);

        savedInstance = new Trinity(psqlConcurrentSnapshots, luceneStorageSystem, storageAPI);
        return savedInstance;
    }

    static ManualTrinity manualConsumerInitialization() throws IOException {
        if (savedInstanceManual != null) {
            LOGGER.info("Returning MANUAL saved instance!");
            return savedInstanceManual;
        }

        var psqlFactory = new PsqlStorageSystemsFactory(
            new LoopingConsumer<>(new DummyConsumer("psql")), Constants.PSQL_LISTEN_PORT_ALT);
        var psqlStorageSystem = psqlFactory.concurSchedule();

        var luceneFactory = new LuceneStorageSystemFactory(
            new LoopingConsumer<>(new DummyConsumer("lucene")),
            Constants.TEST_LUCENE_PSQL_CONTACT_ENDPOINT_ALT);
        JointStorageSystem luceneStorageSystem = luceneFactory.concurSchedule();

        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = StorageAPIUtils.StorageAPIInitArgs.customValues(
            Constants.TEST_KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_PORT_ALT);
        StorageAPI storageAPI = StorageAPIUtils.initFromArgsWithDummyKafkaForTests(storageAPIInitArgs);

        savedInstanceManual = new ManualTrinity(psqlStorageSystem, luceneStorageSystem, storageAPI,
            psqlFactory.getManualConsumer(), luceneFactory.getManualConsumer());

        return savedInstanceManual;
    }

    static void letThatSinkIn(StorageAPI storageAPI, Runnable r) throws InterruptedException {
        r.run();
        storageAPI.waitForAllConfirmations();
    }

    static void letThatSinkInManually(ManualTrinity manualTrinity, Runnable r) {
        r.run();
        int cntPsql = manualTrinity.progressPsql();
        int cntLucene = manualTrinity.progressLucene();

        LOGGER.info(String.format("Manually consumed %d PSQL and %d Lucene messages", cntPsql, cntLucene));
    }
}
