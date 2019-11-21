import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.Consumer;

import java.io.IOException;
import java.net.InetSocketAddress;
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
            new LoopingConsumer<>(kafkaConsumer);

        HttpServer httpServer = HttpServer.create(new InetSocketAddress(argListeningPort), 0);

        Properties props = new Properties();
        props.setProperty("user", argUserPass[0]);
        props.setProperty("password", argUserPass[1]);
        Connection conn = DriverManager.getConnection(argPsqlAddress, props);
        PsqlWrapper psqlWrapper = new PsqlWrapper(conn);

        Gson gson = new Gson();

        PsqlStorageSystem luceneStorageSystem =
            new PsqlStorageSystem(loopingConsumer, httpServer, psqlWrapper, gson);

        // Listen for requests & consume from Kafka topic
        httpServer.setExecutor(null); // creates a default executor
        httpServer.start();

        loopingConsumer.listenBlockingly();
    }
}
