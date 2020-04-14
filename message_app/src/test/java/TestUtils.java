import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.logging.Logger;

class TestUtils {
    private static final Logger LOGGER = Logger.getLogger(TestUtils.class.getName());

    private static ManualTrinity savedInstanceManual;

    private final static Function<StorageSystemFactory, JointStorageSystem> storageSystemStrategy =
        (StorageSystemFactory::concurReads);

    private static class Trinity implements AutoCloseable{
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
        public final ManualConsumer manualConsumerPsql;
        public final ManualConsumer manualConsumerLucene;
        private final ManualConsumer manualConsumerVavr;

        ManualTrinity(JointStorageSystem psqlConcurReads,
                      JointStorageSystem luceneConcurReads,
                      JointStorageSystem vavrStorageSystem,
                      StorageAPI storageAPI,
                      ManualConsumer manualConsumerPsql,
                      ManualConsumer manualConsumerLucene,
                      ManualConsumer manualConsumerVavr) {
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

        public int progressAll() {
            return progressPsql() + progressLucene() + progressVavr();
        }

        public void moveAllToLatest() {
            manualConsumerPsql.moveAllToLatest();
            manualConsumerLucene.moveAllToLatest();
            manualConsumerVavr.moveAllToLatest();
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

    static ManualTrinity manualConsumerInitialization() throws IOException {
        if (savedInstanceManual != null) {
            LOGGER.info("Returning MANUAL saved instance!");
            return savedInstanceManual;
        }

        final ManualConsumer[] psqlManualConsumer = new ManualConsumer[1];
        var psqlFactory = new PsqlStorageSystemsFactory(ConstantsMAPP.PSQL_LISTEN_PORT, (
            jointStorageSystem -> {
                psqlManualConsumer[0] = new ManualConsumer(new DummyConsumer("psql"));
                psqlManualConsumer[0].subscribe(jointStorageSystem::kafkaServiceHandler);
            }));
        var psqlStorageSystem = storageSystemStrategy.apply(psqlFactory);

        final ManualConsumer[] luceneManualConsumer = new ManualConsumer[1];
        var luceneFactory = new LuceneStorageSystemFactory(ConstantsMAPP.TEST_LUCENE_PSQL_CONTACT_ENDPOINT,
            jointStorageSystem -> {
                luceneManualConsumer[0] = new ManualConsumer(new DummyConsumer("lucene"));
                luceneManualConsumer[0].subscribe(jointStorageSystem::kafkaServiceHandler);
            });
        JointStorageSystem luceneStorageSystem = storageSystemStrategy.apply(luceneFactory);

        final ManualConsumer[] vavrManualConsumer = new ManualConsumer[1];
        var vavrFactory = new VavrStorageSystemFactory(ConstantsMAPP.VAVR_LISTEN_PORT,
            jointStorageSystem -> {
                vavrManualConsumer[0] = new ManualConsumer(new DummyConsumer("vavr"));
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

        deleteAllMessages();
        return savedInstanceManual;
    }

    static void letThatSinkIn(ManualTrinity manualTrinity, Runnable r) {
        r.run();
        manualTrinity.progressPsql();
        manualTrinity.progressLucene();
        manualTrinity.progressVavr();
    }

    static <T>T request(BaseEvent event, Class<T> responseType) throws ExecutionException, InterruptedException {
        CompletableFuture<T> response = savedInstanceManual.storageAPI.handleRequest(event, responseType);
        savedInstanceManual.progressPsql();
        savedInstanceManual.progressLucene();
        savedInstanceManual.progressVavr();

        return response.get();
    }

    static void request(BaseEvent event) {
        savedInstanceManual.storageAPI.handleRequest(event);

        savedInstanceManual.progressPsql();
        savedInstanceManual.progressLucene();
        savedInstanceManual.progressVavr();
    }

    static void postMessage(Message message) throws ExecutionException, InterruptedException {
        request(new RequestPostMessage(new Addressable(savedInstanceManual.storageAPI.getResponseAddress()),
            message, ConstantsMAPP.DEFAULT_USER));
    }

    static void postMessage(Message message, String targetRecipient) throws ExecutionException, InterruptedException {
        request(new RequestPostMessage(new Addressable(savedInstanceManual.storageAPI.getResponseAddress()),
            message, targetRecipient));
    }

    static void deleteAllMessages() {
        request(new RequestDeleteAllMessages(new Addressable(savedInstanceManual.storageAPI.getResponseAddress())));
    }

    static ResponseSearchMessage searchMessage(String searchText) throws ExecutionException, InterruptedException {
        return request(new RequestSearchMessage(
            new Addressable(savedInstanceManual.storageAPI.getResponseAddress()), searchText),
            ResponseSearchMessage.class);
    }

    static ResponseMessageDetails messageDetails(long messageID) throws ExecutionException, InterruptedException {
        return request(new RequestMessageDetails(
            new Addressable(savedInstanceManual.storageAPI.getResponseAddress()), messageID),
            ResponseMessageDetails.class);
    }

    static ResponseAllMessages allMessages() throws ExecutionException, InterruptedException {
        return allMessages(ConstantsMAPP.DEFAULT_USER);
    }

    static ResponseAllMessages allMessages(String requester) throws ExecutionException, InterruptedException {
        return request(new RequestAllMessages(
                new Addressable(savedInstanceManual.storageAPI.getResponseAddress()), requester),
            ResponseAllMessages.class);
    }

    static ResponseMessageDetails searchAndDetails(String searchText) throws ExecutionException, InterruptedException {
        return request(new RequestSearchAndDetails(
                new Addressable(savedInstanceManual.storageAPI.getResponseAddress()), searchText),
            ResponseMessageDetails.class);
    }

    static Integer getUnreads(String ofUser) throws ExecutionException, InterruptedException {
        return request(new RequestGetUnreadMessages(
            new Addressable(savedInstanceManual.storageAPI.getResponseAddress()), ofUser), Integer.class);
    }
}
