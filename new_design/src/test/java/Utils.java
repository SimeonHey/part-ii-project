import java.io.IOException;
import java.util.function.Function;
import java.util.logging.Logger;

class Utils {
    private static final Logger LOGGER = Logger.getLogger(Utils.class.getName());

    private static ManualTrinity savedInstanceManual;
    private static Trinity savedInstance;

    private final static Function<StorageSystemFactory, JointStorageSystem> storageSystemStrategy =
        (StorageSystemFactory::simpleOlep);

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

            this.storageAPI.handleRequest(new RequestDeleteAllMessages(
                new Addressable(storageAPI.getResponseAddress()))); // Produces a Kafka message
            this.storageAPI.waitForAllConfirmations();
            Thread.sleep(1000);

            /*this.storageAPI.close();
            this.psqlStorageSystem.close();
            this.luceneStorageSystem.close();
            Thread.sleep(1000);*/
        }
    }

    static class ManualTrinity extends Trinity {
        public final ManualConsumer<Long, BaseEvent> manualConsumerPsql;
        public final ManualConsumer<Long, BaseEvent> manualConsumerLucene;

        ManualTrinity(JointStorageSystem psqlConcurReads,
                      JointStorageSystem luceneConcurReads,
                      StorageAPI storageAPI,
                      ManualConsumer<Long, BaseEvent> manualConsumerPsql,
                      ManualConsumer<Long, BaseEvent> manualConsumerLucene) {
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

        var psqlFactory = new PsqlStorageSystemsFactory(Constants.PSQL_LISTEN_PORT);
        var psqlConcurrentSnapshots = storageSystemStrategy.apply(psqlFactory);

        var luceneFactory = new LuceneStorageSystemFactory(Constants.TEST_LUCENE_PSQL_CONTACT_ENDPOINT);
        JointStorageSystem luceneStorageSystem = storageSystemStrategy.apply(luceneFactory);

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

        final ManualConsumer[] psqlManualConsumer = new ManualConsumer[1];
        var psqlFactory = new PsqlStorageSystemsFactory(Constants.PSQL_LISTEN_PORT_ALT, (
            jointStorageSystem -> {
                psqlManualConsumer[0] = new ManualConsumer<>(new DummyConsumer("psql"));
                psqlManualConsumer[0].subscribe(jointStorageSystem::kafkaServiceHandler);
            }));
        var psqlStorageSystem = storageSystemStrategy.apply(psqlFactory);

        final ManualConsumer[] luceneManualConsumer = new ManualConsumer[1];
        var luceneFactory = new LuceneStorageSystemFactory(Constants.TEST_LUCENE_PSQL_CONTACT_ENDPOINT_ALT,
            jointStorageSystem -> {
                luceneManualConsumer[0] = new ManualConsumer<>(new DummyConsumer("lucene"));
                luceneManualConsumer[0].subscribe(jointStorageSystem::kafkaServiceHandler);
            });

        JointStorageSystem luceneStorageSystem = storageSystemStrategy.apply(luceneFactory);

        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = StorageAPIUtils.StorageAPIInitArgs.customValues(
            Constants.TEST_KAFKA_ADDRESS,
            Constants.KAFKA_TOPIC,
            Constants.STORAGEAPI_PORT_ALT);
        StorageAPI storageAPI = StorageAPIUtils.initFromArgsWithDummyKafkaForTests(storageAPIInitArgs);

        savedInstanceManual = new ManualTrinity(psqlStorageSystem, luceneStorageSystem, storageAPI,
            psqlManualConsumer[0], luceneManualConsumer[0]);

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
