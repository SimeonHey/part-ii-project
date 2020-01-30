import org.apache.kafka.clients.consumer.Consumer;

import java.io.IOException;

public class PsqlUtils {
    public static class PsqlInitArgs {
        public String psqlAddress = Constants.PSQL_ADDRESS;
        public String[] userPass = Constants.PSQL_USER_PASS;
        public String kafkaAddress = Constants.KAFKA_ADDRESS;
        public String transactionsTopic = Constants.KAFKA_TOPIC;
        public String serverAddress = Constants.STORAGEAPI_ADDRESS;
        public String listeningPort = Constants.PSQL_LISTEN_PORT;
        public int numberOfReaders = Constants.PSQL_MAX_READERS;

        private PsqlInitArgs() {

        }

        public static PsqlInitArgs defaultValues() {
            return customValues(
                Constants.PSQL_ADDRESS,
                Constants.PSQL_USER_PASS,
                Constants.KAFKA_ADDRESS,
                Constants.KAFKA_TOPIC,
                Constants.STORAGEAPI_ADDRESS,
                Constants.PSQL_LISTEN_PORT,
                Constants.PSQL_MAX_READERS);
        }

        public static PsqlInitArgs customValues(String argPsqlAddress,
                                                String[] argUserPass,
                                                String argKafkaAddress,
                                                String argTransactionsTopic,
                                                String argServerAddress,
                                                String argListeningPort,
                                                int numberOfReaders) {
            PsqlInitArgs ret = new PsqlInitArgs();

            ret.psqlAddress = argPsqlAddress;
            ret.userPass = argUserPass;
            ret.kafkaAddress = argKafkaAddress;
            ret.transactionsTopic = argTransactionsTopic;
            ret.serverAddress = argServerAddress;
            ret.listeningPort = argListeningPort;
            ret.numberOfReaders = numberOfReaders;

            return ret;
        }
    }

    public static PsqlConcurrentSnapshots getStorageSystem(PsqlInitArgs initArgs) throws IOException {
        PsqlWrapper psqlWrapper = new PsqlWrapper(() -> SqlUtils.obtainConnection(initArgs.userPass[0],
            initArgs.userPass[1], initArgs.psqlAddress));

        // Initialize the http server
        HttpStorageSystem httpStorageSystem =
            new HttpStorageSystem("psql",
                HttpUtils.initHttpServer(Integer.parseInt(initArgs.listeningPort)));

        PsqlConcurrentSnapshots psqlConcurrentSnapshots =
            new PsqlConcurrentSnapshots(psqlWrapper, initArgs.serverAddress, initArgs.numberOfReaders, httpStorageSystem);
        psqlConcurrentSnapshots.deleteAllMessages();

        return psqlConcurrentSnapshots;
    }

    public static Consumer<Long, StupidStreamObject> getConsumer(PsqlInitArgs initArgs) {
        return KafkaUtils.createConsumer(
            "psql",
            initArgs.kafkaAddress,
            initArgs.transactionsTopic);
    }

}
