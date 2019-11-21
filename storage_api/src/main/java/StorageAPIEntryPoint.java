import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class StorageAPIEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIEntryPoint.class.getName());

    private static HttpURLConnection sendHttpGetRequest(String base,
                                                        String endpoint,
                                                        String params) throws IOException {
        String url = String.format("%s/%s?%s", base, endpoint, params);
        LOGGER.info("Sending an HTTP request to " + url);

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

    private static void discoverEndpoint(String endpoint) throws InterruptedException {
        while (true) {
            String url = String.format("%s/discover", endpoint);
            try {
                HttpURLConnection conn =
                    sendHttpGetRequest(endpoint, "discover", "");
                if (conn.getResponseCode() != 200) {
                    throw new IOException();
                }
                break;
            } catch (IOException e) {
                LOGGER.warning("Couldn't connect to " + url + ". Retrying...");
                Thread.sleep(1000);
            }
        }
    }

    private static void produceMessage(Producer<Long, StupidStreamObject> producer,
                                       StupidStreamObject toSend) {
        ProducerRecord<Long, StupidStreamObject> record = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, toSend);
        try {
            RecordMetadata metadata = producer.send(record).get();
            LOGGER.info("Produced message of type " + toSend.getObjectType()
                + " with Kafka offset = " + metadata.offset());
        }
        catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // Consume command line arguments
        String argLuceneAddress = args[0];
        String argPsqlAddress = args[1];

        LOGGER.info("Starting StorageAPI with params " + Arrays.toString(args));

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer = KafkaUtils.createProducer();
        LOGGER.info("Success");

        LOGGER.info("Connecting to Lucene...");
        discoverEndpoint(argLuceneAddress);
        LOGGER.info("Success");

        LOGGER.info("Connecting to PSQL...");
        discoverEndpoint(argPsqlAddress);
        LOGGER.info("Success");

        // Take user commands and perform actions
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter query:");
            String[] line;

            try {
                line = scanner.nextLine().split(" ");
            } catch (NoSuchElementException e) {
                LOGGER.info("End of user input.. breaking out of the loop");
                break;
            }

            LOGGER.info("Got " + Arrays.toString(line));

            switch (line[0]) {
                case "post":
                    produceMessage(producer, PostMessageRequest.toStupidStreamObject(line[1], line[2]));
                    break;
                case "search":
                    String resp1 = httpRequestResponse(argLuceneAddress, "search", line[1]);
                    System.out.println("Search response: " + resp1);
                    break;
                case "details":
                    String resp2 = httpRequestResponse(argPsqlAddress, "messageDetails", line[1]);
                    System.out.println("Message details: " + resp2);
                    break;
                default:
                    System.out.println("Couldn't catch that");
            }
        }
    }
}
