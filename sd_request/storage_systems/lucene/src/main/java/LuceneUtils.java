import org.apache.kafka.clients.consumer.Consumer;

public class LuceneUtils {
    public static class LuceneInitArgs {
        public final String argKafkaAddress;
        public final String argTransactionsTopic;
        public final String argServerAddress;
        public final String argPsqlContactAddress;

        public LuceneInitArgs(String[] args) {
            argKafkaAddress = args[0];
            argTransactionsTopic = args[1];
            argServerAddress = args[2];
            argPsqlContactAddress = args[3];
        }

        public LuceneInitArgs(String argKafkaAddress, String argTransactionsTopic, String argServerAddress,
                              String argPsqlContactAddress) {
            this.argKafkaAddress = argKafkaAddress;
            this.argTransactionsTopic = argTransactionsTopic;
            this.argServerAddress = argServerAddress;
            this.argPsqlContactAddress = argPsqlContactAddress;
        }
    }

    static LuceneStorageSystem getStorageSystem(LuceneInitArgs initArgs) {
        LuceneStorageSystem luceneStorageSystem =
            new LuceneStorageSystem(new LuceneWrapper(), initArgs.argServerAddress, initArgs.argPsqlContactAddress);
        luceneStorageSystem.deleteAllMessages();

        return luceneStorageSystem;
    }

    static Consumer<Long, StupidStreamObject> getConsumer(LuceneInitArgs initArgs) {
        return KafkaUtils.createConsumer(
            "lucene",
            initArgs.argKafkaAddress,
            initArgs.argTransactionsTopic);
    }
}
