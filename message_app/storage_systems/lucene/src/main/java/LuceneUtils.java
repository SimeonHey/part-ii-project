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
                ConstantsMAPP.TEST_KAFKA_ADDRESS,
                ConstantsMAPP.KAFKA_TOPIC,
                ConstantsMAPP.TEST_STORAGEAPI_ADDRESS,
                ConstantsMAPP.TEST_LUCENE_PSQL_CONTACT_ENDPOINT,
                ConstantsMAPP.LUCENE_MAX_READERS);
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
