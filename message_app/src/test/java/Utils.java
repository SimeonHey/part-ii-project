import java.io.IOException;
import java.util.function.Function;
import java.util.logging.Logger;

class Utils {
    private static final Logger LOGGER = Logger.getLogger(Utils.class.getName());

    private static ManualTrinity savedInstanceManual;
    private static Trinity savedInstance;

    private final static Function<StorageSystemFactory, JointStorageSystem> storageSystemStrategy =
        (StorageSystemFactory::concurReads);

    static class Trinity implements AutoCloseable{
        public final JointStorageSystem psqlStorageSystem;
        public final JointStorageSystem luceneStorageSystem;
        public final StorageAPI storageAPI;
        protected JointStorageSystem vavrStorageSystem;

        Trinity(JointStorageSystem psqlStorageSystem,
                JointStorageSystem luceneStorageSystem,
                JointStorageSystem vavrStorageSystem,
                StorageAPI storageAPI) {
            this.psqlStorageSystem = psqlStorageSystem;
            this.luceneStorageSystem = luceneStorageSystem;
            this.vavrStorageSystem = vavrStorageSystem;

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
        private final ManualConsumer<Long, BaseEvent> manualConsumerVavr;

        ManualTrinity(JointStorageSystem psqlConcurReads,
                      JointStorageSystem luceneConcurReads,
                      JointStorageSystem vavrStorageSystem,
                      StorageAPI storageAPI,
                      ManualConsumer<Long, BaseEvent> manualConsumerPsql,
                      ManualConsumer<Long, BaseEvent> manualConsumerLucene,
                      ManualConsumer<Long, BaseEvent> manualConsumerVavr) {
            super(psqlConcurReads, luceneConcurReads, vavrStorageSystem, storageAPI);
            this.manualConsumerPsql = manualConsumerPsql;
            this.manualConsumerLucene = manualConsumerLucene;
            this.manualConsumerVavr = manualConsumerVavr;
        }

        public int progressPsql() {
            return manualConsumerPsql.consumeAvailableRecords();
        }

        public int progressLucene() {
            return manualConsumerLucene.consumeAvailableRecords();
        }

        public int progressVavr() {
            return manualConsumerVavr.consumeAvailableRecords();
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
            this.manualConsumerVavr.close();

            this.storageAPI.close();
            this.psqlStorageSystem.close();
            this.luceneStorageSystem.close();
            this.vavrStorageSystem.close();
        }
    }

    static Trinity basicInitialization() throws IOException, InterruptedException {
        if (savedInstance != null) {
            LOGGER.info("Returning saved instance!");
            return savedInstance;
        }

        var psqlFactory = new PsqlStorageSystemsFactory(ConstantsMAPP.PSQL_LISTEN_PORT);
        var psqlConcurrentSnapshots = storageSystemStrategy.apply(psqlFactory);

        var luceneFactory = new LuceneStorageSystemFactory(ConstantsMAPP.TEST_LUCENE_PSQL_CONTACT_ENDPOINT);
        JointStorageSystem luceneStorageSystem = storageSystemStrategy.apply(luceneFactory);

        var vavrFactory = new VavrStorageSystemFactory(ConstantsMAPP.VAVR_LISTEN_PORT);
        JointStorageSystem vavrStorageSystem = storageSystemStrategy.apply(vavrFactory);

        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = StorageAPIUtils.StorageAPIInitArgs.defaultTestValues();
        StorageAPI storageAPI = StorageAPIUtils.initFromArgsForTests(storageAPIInitArgs);

        Thread.sleep(1000);

        savedInstance = new Trinity(psqlConcurrentSnapshots, luceneStorageSystem, vavrStorageSystem, storageAPI);
        return savedInstance;
    }

    static ManualTrinity manualConsumerInitialization() throws IOException {
        if (savedInstanceManual != null) {
            LOGGER.info("Returning MANUAL saved instance!");
            return savedInstanceManual;
        }

        final ManualConsumer[] psqlManualConsumer = new ManualConsumer[1];
        var psqlFactory = new PsqlStorageSystemsFactory(ConstantsMAPP.PSQL_LISTEN_PORT, (
            jointStorageSystem -> {
                psqlManualConsumer[0] = new ManualConsumer<>(new DummyConsumer("psql"));
                psqlManualConsumer[0].subscribe(jointStorageSystem::kafkaServiceHandler);
            }));
        var psqlStorageSystem = storageSystemStrategy.apply(psqlFactory);

        final ManualConsumer[] luceneManualConsumer = new ManualConsumer[1];
        var luceneFactory = new LuceneStorageSystemFactory(ConstantsMAPP.TEST_LUCENE_PSQL_CONTACT_ENDPOINT,
            jointStorageSystem -> {
                luceneManualConsumer[0] = new ManualConsumer<>(new DummyConsumer("lucene"));
                luceneManualConsumer[0].subscribe(jointStorageSystem::kafkaServiceHandler);
            });
        JointStorageSystem luceneStorageSystem = storageSystemStrategy.apply(luceneFactory);

        final ManualConsumer[] vavrManualConsumer = new ManualConsumer[1];
        var vavrFactory = new VavrStorageSystemFactory(ConstantsMAPP.VAVR_LISTEN_PORT,
            jointStorageSystem -> {
                System.out.println("Created the vavr shit");
                vavrManualConsumer[0] = new ManualConsumer<>(new DummyConsumer("vavr"));
                vavrManualConsumer[0].subscribe(jointStorageSystem::kafkaServiceHandler);
            });
        JointStorageSystem vavrStorageSystem = storageSystemStrategy.apply(vavrFactory);


        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = StorageAPIUtils.StorageAPIInitArgs.customValues(
            ConstantsMAPP.TEST_KAFKA_ADDRESS,
            ConstantsMAPP.KAFKA_TOPIC,
            ConstantsMAPP.STORAGEAPI_PORT);
        StorageAPI storageAPI = StorageAPIUtils.initFromArgsWithDummyKafkaForTests(storageAPIInitArgs);

        savedInstanceManual = new ManualTrinity(psqlStorageSystem, luceneStorageSystem, vavrStorageSystem, storageAPI,
            psqlManualConsumer[0], luceneManualConsumer[0], vavrManualConsumer[0]);

        return savedInstanceManual;
    }

    static void letThatSinkIn(StorageAPI storageAPI, Runnable r) {
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
