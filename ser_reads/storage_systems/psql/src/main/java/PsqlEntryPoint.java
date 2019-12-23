import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.Consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PsqlEntryPoint {
    public static void main(String[] args) throws SQLException {
        // Consume program line arguments
        String argPsqlAddress = args[0];
        String[] argUserPass = args[1].split(":");
        String argKafkaAddress = args[2];
        String argTransactionsTopic = args[3];
        String argServerAddress = args[4];

        // Connect to Kafka & PSQL
        Consumer<Long, StupidStreamObject> kafkaConsumer = KafkaUtils.createConsumer(
            "psql",
            argKafkaAddress,
            argTransactionsTopic);
        LoopingConsumer<Long, StupidStreamObject> loopingConsumer =
            new LoopingConsumer<>(kafkaConsumer, 100);

        // Initialize Database connection
        Properties props = new Properties();
        props.setProperty("user", argUserPass[0]);
        props.setProperty("password", argUserPass[1]);
        Connection conn = DriverManager.getConnection(argPsqlAddress, props);
        PsqlWrapper psqlWrapper = new PsqlWrapper(conn);

        PsqlStorageSystem psqlStorageSystem =
            new PsqlStorageSystem(loopingConsumer, psqlWrapper, argServerAddress, new Gson());
        psqlStorageSystem.deleteAllMessages();

        loopingConsumer.listenBlockingly();
    }
}
