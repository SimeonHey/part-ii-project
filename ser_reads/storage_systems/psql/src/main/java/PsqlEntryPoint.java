import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.Consumer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PsqlEntryPoint {
    public static void main(String[] args) throws IOException, SQLException {
        // Consume program line arguments
        int argListeningPort = Integer.parseInt(args[0]);
        String argPsqlAddress = args[1];
        String[] argUserPass = args[2].split(":");
        String argKafkaAddress = args[3];
        String argTransactionsTopic = args[4];

        // Connect to Kafka & PSQL
        Consumer<Long, StupidStreamObject> kafkaConsumer = KafkaUtils.createConsumer(
            "psql",
            argKafkaAddress,
            argTransactionsTopic);
        LoopingConsumer<Long, StupidStreamObject> loopingConsumer =
            new LoopingConsumer<>(kafkaConsumer, 10 * 1000);

        HttpStorageSystem httpStorageSystem =
            new HttpStorageSystem("psql", HttpUtils.initHttpServer(argListeningPort));

        Properties props = new Properties();
        props.setProperty("user", argUserPass[0]);
        props.setProperty("password", argUserPass[1]);
        Connection conn = DriverManager.getConnection(argPsqlAddress, props);
        PsqlWrapper psqlWrapper = new PsqlWrapper(conn);

        Gson gson = new Gson();

        PsqlStorageSystem psqlStorageSystem =
            new PsqlStorageSystem(loopingConsumer, httpStorageSystem, psqlWrapper, gson);
        psqlStorageSystem.deleteAllMessages();

        loopingConsumer.listenBlockingly();
    }
}
