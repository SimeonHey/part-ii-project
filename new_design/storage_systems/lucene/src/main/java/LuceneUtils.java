import org.apache.kafka.clients.consumer.Consumer;

public class LuceneUtils {
    public static class LuceneInitArgs {
        public String kafkaAddress;
        public String transactionsTopic;
        public String serverAddress;
        public String psqlContactAddress;
        public int maxNumberOfReaders;

        private LuceneInitArgs() {

        }

        public static LuceneInitArgs defaultValues() {
            return fromValues(
                Constants.TEST_KAFKA_ADDRESS,
                Constants.KAFKA_TOPIC,
                Constants.TEST_STORAGEAPI_ADDRESS,
                Constants.TEST_LUCENE_PSQL_CONTACT_ENDPOINT,
                Constants.LUCENE_MAX_READERS);
        }

        public static LuceneInitArgs fromValues(String argKafkaAddress,
                                                String argTransactionsTopic,
                                                String argServerAddress,
                                                String argPsqlContactAddress,
                                                int maxNumberOfReaders) {
            LuceneInitArgs ret = new LuceneInitArgs();
            
            ret.kafkaAddress = argKafkaAddress;
            ret.transactionsTopic = argTransactionsTopic;
            ret.serverAddress = argServerAddress;
            ret.psqlContactAddress = argPsqlContactAddress;
            ret.maxNumberOfReaders = maxNumberOfReaders;
            
            return ret;
        }
    }
}
