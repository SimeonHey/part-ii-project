import org.apache.kafka.clients.consumer.Consumer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PsqlUtils {
    public static class PsqlInitArgs {
        public final String argPsqlAddress;
        public final String[] argUserPass;
        public final String argKafkaAddress;
        public final String argTransactionsTopic;
        public final String argServerAddress;
        public final String argListeningPort;

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

    public static PsqlStorageSystem getStorageSystem(PsqlInitArgs initArgs) throws SQLException, IOException {
        // Initialize Database connection
        Properties props = new Properties();
        props.setProperty("user", initArgs.argUserPass[0]);
        props.setProperty("password", initArgs.argUserPass[1]);
        Connection conn = DriverManager.getConnection(initArgs.argPsqlAddress, props);
        PsqlWrapper psqlWrapper = new PsqlWrapper(conn);

        // Initialize the http server
        HttpStorageSystem httpStorageSystem =
            new HttpStorageSystem("psql",
                HttpUtils.initHttpServer(Integer.parseInt(initArgs.argListeningPort)));

        PsqlStorageSystem psqlStorageSystem =
            new PsqlStorageSystem(psqlWrapper, initArgs.argServerAddress, httpStorageSystem);
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
