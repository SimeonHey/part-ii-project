import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.logging.Logger;

class TestUtils {
    private static final Logger LOGGER = Logger.getLogger(TestUtils.class.getName());

    private static ManualTrinity savedInstanceManual;

    private final static Function<StorageSystemFactory, StorageSystem> storageSystemStrategy =
        (StorageSystemFactory::simpleOlep);

    private static class Trinity implements AutoCloseable{
        public final StorageSystem psqlStorageSystem;
        public final StorageSystem luceneStorageSystem;
        public final PolyglotAPI polyglotAPI;
        protected StorageSystem vavrStorageSystem;

        Trinity(StorageSystem psqlStorageSystem,
				StorageSystem luceneStorageSystem,
				StorageSystem vavrStorageSystem,
				PolyglotAPI polyglotAPI) {
            this.psqlStorageSystem = psqlStorageSystem;
            this.luceneStorageSystem = luceneStorageSystem;
            this.vavrStorageSystem = vavrStorageSystem;

            this.polyglotAPI = polyglotAPI;
        }

        @Override
        public void close() throws Exception {
            LOGGER.info("STARTING SHUTDOWN PROCEDURE");

            this.polyglotAPI.handleRequest(new RequestDeleteAllMessages(
                new Addressable(polyglotAPI.getResponseAddress()))); // Produces a Kafka message
            this.polyglotAPI.waitForAllConfirmations();
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

        private final MultithreadedEventQueueExecutor executorPsql =
            new MultithreadedEventQueueExecutor(1, new MultithreadedEventQueueExecutor.FifoScheduler());

        private final MultithreadedEventQueueExecutor executorLucene =
            new MultithreadedEventQueueExecutor(1, new MultithreadedEventQueueExecutor.FifoScheduler());

        private final MultithreadedEventQueueExecutor executorVavr =
            new MultithreadedEventQueueExecutor(1, new MultithreadedEventQueueExecutor.FifoScheduler());

        ManualTrinity(StorageSystem psqlConcurReads,
					  StorageSystem luceneConcurReads,
					  StorageSystem vavrStorageSystem,
					  PolyglotAPI polyglotAPI,
					  ManualConsumer manualConsumerPsql,
					  ManualConsumer manualConsumerLucene,
					  ManualConsumer manualConsumerVavr) {
            super(psqlConcurReads, luceneConcurReads, vavrStorageSystem, polyglotAPI);
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

        public void progressPsqlAsync() {
            executorPsql.submitOperation(manualConsumerPsql::consumeAvailableRecords);
        }

        public void progressLuceneAsync() {
            executorLucene.submitOperation(manualConsumerLucene::consumeAvailableRecords);
        }

        public void progressVavrAsync() {
            executorVavr.submitOperation(manualConsumerVavr::consumeAvailableRecords);
        }

        public void progressAllAsync() {
            progressPsqlAsync();
            progressLuceneAsync();
            progressVavrAsync();
        }

        public void moveAllToLatest() {
            manualConsumerPsql.moveAllToLatest();
            manualConsumerLucene.moveAllToLatest();
            manualConsumerVavr.moveAllToLatest();
        }

        @Override
        public void close() throws Exception {
            LOGGER.info("STARTING SHUTDOWN PROCEDURE");

            this.manualConsumerPsql.close();
            this.manualConsumerLucene.close();
            this.manualConsumerVavr.close();

            this.polyglotAPI.close();
            this.psqlStorageSystem.close();
            this.luceneStorageSystem.close();
            this.vavrStorageSystem.close();
        }
    }

    static ManualTrinity manualConsumerInitialization() throws IOException, InterruptedException, ExecutionException {
        if (savedInstanceManual != null) {
            LOGGER.info("Returning MANUAL saved instance!");
            return savedInstanceManual;
        }

        final ManualConsumer[] psqlManualConsumer = new ManualConsumer[1];
        var psqlFactory = new PsqlStorageSystemsFactory(ConstantsMAPP.PSQL_LISTEN_PORT, (
            jointStorageSystem -> {
                psqlManualConsumer[0] = new ManualConsumer(new DummyConsumer("psql"));
                psqlManualConsumer[0].subscribe(jointStorageSystem::kafkaActionHandler);
            }));
        var psqlStorageSystem = storageSystemStrategy.apply(psqlFactory);

        final ManualConsumer[] luceneManualConsumer = new ManualConsumer[1];
        var luceneFactory = new LuceneStorageSystemFactory(ConstantsMAPP.TEST_LUCENE_PSQL_CONTACT_ENDPOINT,
            jointStorageSystem -> {
                luceneManualConsumer[0] = new ManualConsumer(new DummyConsumer("lucene"));
                luceneManualConsumer[0].subscribe(jointStorageSystem::kafkaActionHandler);
            });
        StorageSystem luceneStorageSystem = storageSystemStrategy.apply(luceneFactory);

        final ManualConsumer[] vavrManualConsumer = new ManualConsumer[1];
        var vavrFactory = new VavrStorageSystemFactory(ConstantsMAPP.VAVR_LISTEN_PORT,
            jointStorageSystem -> {
                vavrManualConsumer[0] = new ManualConsumer(new DummyConsumer("vavr"));
                vavrManualConsumer[0].subscribe(jointStorageSystem::kafkaActionHandler);
            });
        StorageSystem vavrStorageSystem = storageSystemStrategy.apply(vavrFactory);


        StorageAPIUtils.StorageAPIInitArgs storageAPIInitArgs = StorageAPIUtils.StorageAPIInitArgs.customValues(
            ConstantsMAPP.TEST_KAFKA_ADDRESS,
            ConstantsMAPP.KAFKA_TOPIC,
            ConstantsMAPP.STORAGEAPI_PORT);
        PolyglotAPI polyglotAPI = StorageAPIUtils.initFromArgsWithDummyKafkaForTests(storageAPIInitArgs);

        savedInstanceManual = new ManualTrinity(psqlStorageSystem, luceneStorageSystem, vavrStorageSystem, polyglotAPI,
            psqlManualConsumer[0], luceneManualConsumer[0], vavrManualConsumer[0]);

        deleteAllMessages();
        return savedInstanceManual;
    }

    static <T>T request(EventBase event, Class<T> responseType) throws ExecutionException, InterruptedException {
        CompletableFuture<T> response = savedInstanceManual.polyglotAPI.handleRequest(event, responseType);
        savedInstanceManual.progressAll();

        return response.get();
    }

    static void request(EventBase event) throws InterruptedException, ExecutionException {
        var future = savedInstanceManual.polyglotAPI.handleRequest(event);

        savedInstanceManual.progressAll();

        var confirmation = future.get();
        LOGGER.info("Utils class successfully posted request " + event + " and got confirmation " + confirmation);
    }

    static void requestNoWait(EventBase event) throws InterruptedException, ExecutionException {
        System.out.println("DOING IT NO WAITING: " + event);
        savedInstanceManual.polyglotAPI.handleRequest(event);
        savedInstanceManual.progressAllAsync();
        System.out.println("DID IT NO WAITING: " + event);
    }

    static void postMessage(Message message) throws ExecutionException, InterruptedException {
        request(new RequestPostMessage(new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress()), message));
    }

    static void deleteAllMessages() throws InterruptedException, ExecutionException {
        request(new RequestDeleteAllMessages(new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress())));
    }

    static void deleteConversation(String user1, String user2) throws InterruptedException, ExecutionException {
        request(new RequestDeleteConversation(new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress()),
            user1, user2));
    }

    static ResponseSearchMessage searchMessage(String searchText) throws ExecutionException, InterruptedException {
        return request(new RequestSearchMessage(
            new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress()), searchText),
            ResponseSearchMessage.class);
    }

    static ResponseMessageDetails messageDetails(long messageID) throws ExecutionException, InterruptedException {
        return request(new RequestMessageDetails(
            new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress()), messageID),
            ResponseMessageDetails.class);
    }

    static ResponseAllMessages getAllConvoMessages(String requester, String withUser) throws ExecutionException,
        InterruptedException {
        return request(new RequestConvoMessages(
                new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress()), requester, withUser),
            ResponseAllMessages.class);
    }

    static ResponseMessageDetails searchAndDetails(String searchText) throws ExecutionException, InterruptedException {
        return request(new RequestSearchAndGetDetails(
                new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress()), searchText),
            ResponseMessageDetails.class);
    }

    static Integer getAllMessages(String ofUser1, String ofUser2) throws ExecutionException, InterruptedException {
        return request(new RequestGetTotalNumberOfMessages(
            new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress()), ofUser1, ofUser2), Integer.class);
    }

    static void sleep1(boolean wait) throws ExecutionException, InterruptedException {
        if (wait) {
            request(new RequestSleep1(new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress())));
        } else {
            requestNoWait(new RequestSleep1(new Addressable(savedInstanceManual.polyglotAPI.getResponseAddress())));
        }
    }
}
