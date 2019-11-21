import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ConsistencyTests {
    @Test
    public void shouldWork() throws SQLException {
        ManualConsumer<Long, StupidStreamObject> consumerLucene =
            new ManualConsumer<>(KafkaUtils.createConsumer(
                "lucene",
                "localhost:9092",
                "transactions"));
        ManualConsumer<Long, StupidStreamObject> consumerPsql =
            new ManualConsumer<>(KafkaUtils.createConsumer(
                "psql",
                "localhost:9092",
                "transactions"));

        consumerLucene.moveAllToLatest();
        consumerPsql.moveAllToLatest();

        Producer<Long, StupidStreamObject> producer =
            KafkaUtils.createProducer("localhost:9092", "mainThing");

        HttpServer mockedHttpServer = Mockito.mock(HttpServer.class);

        LuceneWrapper luceneWrapper = new LuceneWrapper();
        Gson gson = new Gson();
        LuceneStorageSystem luceneStorageSystem =
            new LuceneStorageSystem(consumerLucene, mockedHttpServer, luceneWrapper, gson);

        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "default");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/simple_olep_test", props);
        SqlUtils.executeStatement("DELETE FROM messages", conn);
        PsqlWrapper psqlWrapper = new PsqlWrapper(conn);
        PsqlStorageSystem psqlStorageSystem =
            new PsqlStorageSystem(consumerPsql, mockedHttpServer, psqlWrapper, gson);

        KafkaUtils.produceMessage(producer, "transactions",
            RequestPostMessage.toStupidStreamObject("simeon", "hello bud"));
        assertEquals(1, consumerLucene.consumeAvailableRecords());

        ResponseSearchMessage response =
            luceneWrapper.searchMessage(new RequestSearchMessage("hello bud"));
        assertEquals(1, response.getOccurrences().size());

        long id = response.getOccurrences().get(0);

        // Uncomment the line below to fix test
        // consumerPsql.consumeAvailableRecords();

        consumerLucene.close();
        consumerPsql.close();

        String details = psqlWrapper.getMessageDetails(new RequestMessageDetails(id));
        assertEquals(String.format("simeon hello bud %d ", id), details);
    }
}
