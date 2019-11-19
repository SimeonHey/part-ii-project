import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class StorageAPIEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIEntryPoint.class.getName());

    private static HttpURLConnection sendHttpGetRequest(String base,
                                                        String endpoint,
                                                        String params) throws IOException {
        String url = String.format("%s/%s?%s", base, endpoint, params);
        LOGGER.info("URL is " + url);
        HttpURLConnection httpURLConnection =
            (HttpURLConnection) new URL(url).openConnection();
        httpURLConnection.setRequestMethod("GET");

        return httpURLConnection;
    }

    private static String httpRequestResponse(String base,
                                              String endpoint,
                                              String params) throws IOException {
        HttpURLConnection conn = sendHttpGetRequest(base, endpoint, params);
        return new String(conn.getInputStream().readAllBytes());
    }

    private static void produceMessage(Producer<Long, StupidStreamObject> producer,
                                       StupidStreamObject toSend) {
        ProducerRecord<Long, StupidStreamObject> record = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, toSend);
        try {
            RecordMetadata metadata = producer.send(record).get();
            /*LOGGER.info("Record sent with key " + index + " to partition " + metadata.partition()
                + " with offset " + metadata.offset());*/
        }
        catch (ExecutionException | InterruptedException e) {
            LOGGER.info("Error in sending record");
            LOGGER.info(e.toString());
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        // Consume command line arguments
        String argLuceneAddress = args[0];
        String argPsqlAddress = args[1];

        // Setup connection with Kafka, possibly storage systems too
        Producer<Long, StupidStreamObject> producer = KafkaUtils.createProducer();

        HttpURLConnection conLucene =
            sendHttpGetRequest(argLuceneAddress, "lucene/discover", "");
        if (conLucene.getResponseCode() != 200) {
            throw new RuntimeException("Couldn't discover lucene");
        }
        LOGGER.info("Connected to Lucene");

        HttpURLConnection conPsql =
            sendHttpGetRequest(argPsqlAddress, "psql/discover", "");
        if (conPsql.getResponseCode() != 200) {
            throw new RuntimeException("Couldn't discover lucene");
        }
        LOGGER.info("Connected to PSQL");

        // Start listening for queries
        // Produce queries to Kafka topics
        // Possibly listen for answers
        Scanner scanner = new Scanner(System.in);
        long uuid = 0; // TODO: come up with a real uuid

        while (true) {
            System.out.println("Enter query:");
            String[] line = scanner.nextLine().split(" ");
            LOGGER.info("Got " + Arrays.toString(line));

            switch (line[0]) {
                case "post":
                    produceMessage(producer, PostMessageRequest.toStupidStreamObject(line[1], line[2]));
                    break;
                case "search":
                    String resp1 = httpRequestResponse(argLuceneAddress, "lucene/search", line[1]);
                    System.out.println("Search response: " + resp1);
                    break;
                case "details":
                    String resp2 = httpRequestResponse(argPsqlAddress, "psql/messageDetails", line[1]);
                    System.out.println("Message details: " + resp2);
                    break;
                default:
                    System.out.println("Couldn't catch that");
            }
        }
    }
}
