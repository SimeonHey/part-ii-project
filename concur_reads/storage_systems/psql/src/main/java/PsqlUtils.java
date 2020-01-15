import org.apache.kafka.clients.consumer.Consumer;

import java.io.IOException;

public class PsqlUtils {
    public static class PsqlInitArgs {
        public final String argPsqlAddress;
        public final String[] argUserPass;
        public final String argKafkaAddress;
        public final String argTransactionsTopic;
        public final String argServerAddress;
        public final String argListeningPort;

        public int numberOfReaders = Constants.PSQL_DEFAULT_READER_THREADS;

        public PsqlInitArgs(String[] args) {
            this.argPsqlAddress = args[0];
            this.argUserPass = args[1].split(":");
            this.argKafkaAddress = args[2];
            this.argTransactionsTopic = args[3];
            this.argServerAddress = args[4];
            this.argListeningPort = args[5];
        }

        public PsqlInitArgs(String argPsqlAddress,
                            String[] argUserPass,
                            String argKafkaAddress,
                            String argTransactionsTopic,
                            String argServerAddress,
                            String argListeningPort) {
            this.argPsqlAddress = argPsqlAddress;
            this.argUserPass = argUserPass;
            this.argKafkaAddress = argKafkaAddress;
            this.argTransactionsTopic = argTransactionsTopic;
            this.argServerAddress = argServerAddress;
            this.argListeningPort = argListeningPort;
        }
    }

    public static PsqlStorageSystem getStorageSystem(PsqlInitArgs initArgs) throws IOException {
        PsqlWrapper psqlWrapper = new PsqlWrapper(() -> SqlUtils.obtainConnection(initArgs.argUserPass[0],
            initArgs.argUserPass[1], initArgs.argPsqlAddress));

        // Initialize the http server
        HttpStorageSystem httpStorageSystem =
            new HttpStorageSystem("psql",
                HttpUtils.initHttpServer(Integer.parseInt(initArgs.argListeningPort)));

        PsqlStorageSystem psqlStorageSystem =
            new PsqlStorageSystem(psqlWrapper, initArgs.argServerAddress, initArgs.numberOfReaders, httpStorageSystem);
        psqlStorageSystem.deleteAllMessages();

        return psqlStorageSystem;
    }

    public static Consumer<Long, StupidStreamObject> getConsumer(PsqlInitArgs initArgs) {
        return KafkaUtils.createConsumer(
            "psql",
            initArgs.argKafkaAddress,
            initArgs.argTransactionsTopic);
    }

}
