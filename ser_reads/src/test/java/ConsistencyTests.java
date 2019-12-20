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
        String kafkaTopic = "transactions";

        String messageSender = "simeon";
        String messageText = "hello bud";

        // Initialization part

        // Manual consumers for lucene and psql
        // Using a dummy implementation of Kafak
        ManualConsumer<Long, StupidStreamObject> consumerLucene =
            new ManualConsumer<>(new DummyConsumer());
        ManualConsumer<Long, StupidStreamObject> consumerPsql =
            new ManualConsumer<>(new DummyConsumer());

        consumerLucene.moveAllToLatest();
        consumerPsql.moveAllToLatest();

        // Manual producer
        Producer<Long, StupidStreamObject> producer = new DummyProducer();

        // Initialize Lucene
        LuceneWrapper luceneWrapper = new LuceneWrapper();
        Gson gson = new Gson();
        new LuceneStorageSystem(consumerLucene, Mockito.mock(HttpServer.class), luceneWrapper, gson, serverAddress);

        // Initialize PostgreSQL
        // TODO: *Consider* mocking it
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "default");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/ser_reads_test", props);

        PsqlWrapper psqlWrapper = new PsqlWrapper(conn);
        new PsqlStorageSystem(consumerPsql, Mockito.mock(HttpServer.class), psqlWrapper, gson);

        // Behaviour part starts below

        // Reset what we have
        luceneWrapper.deleteAllMessages();
        psqlWrapper.deleteAllMessages();

        // Produce a message
        Message message = new Message(messageSender, messageText);
        RequestPostMessage requestPostMessage = new RequestPostMessage(message);
        KafkaUtils.produceMessage(producer, kafkaTopic, requestPostMessage.toStupidStreamObject());

        // Consume the message manually with Lucene
        assertEquals(1, consumerLucene.consumeAvailableRecords());
        ResponseSearchMessage response =
            luceneWrapper.searchMessage(new RequestSearchMessage(messageText, responseEndpoint));
        assertEquals(1, response.getOccurrences().size());

        long id = response.getOccurrences().get(0);

        // Uncomment the line below to fix test
        consumerPsql.consumeAvailableRecords();

        consumerLucene.close();
        consumerPsql.close();

        // Assertion part

        ResponseMessageDetails details =
            psqlWrapper.getMessageDetails(new RequestMessageDetails(id));
        assertEquals(details,
            new ResponseMessageDetails(new Message(messageSender, messageText), id));
    }
}
