import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import scala.reflect.internal.pickling.UnPickler;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class EntryPoint {

    private static HttpURLConnection sendHttpGetRequest(String base,
                                                        String endpoint,
                                                        String params) throws IOException {
        String url = String.format("%s/%s?%s", base, endpoint, params);
        System.out.println("URL is " + url);
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
            /*System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                + " with offset " + metadata.offset());*/
        }
        catch (ExecutionException | InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
    }

    public static void main(String[] args) throws IOException {
        // Consume command line arguments
        String argLuceneAddress = args[0];

        // Setup connection with Kafka, possibly storage systems too
        Producer<Long, StupidStreamObject> producer = KafkaUtils.createProducer();

        HttpURLConnection con =
            sendHttpGetRequest(argLuceneAddress, "lucene/discover", "");
        if (con.getResponseCode() != 200) {
            throw new RuntimeException("Couldn't discover lucene");
        }

        System.out.println("Lucene search response: " +
            httpRequestResponse(argLuceneAddress, "lucene/search", "paraaaaam"));

        // Start listening for queries
        // Produce queries to Kafka topics
        // Possibly listen for answers
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String[] line = scanner.nextLine().split(" ");
            System.out.println("Got " + Arrays.toString(line));

            switch (line[0]) {
                case "post":
                    produceMessage(producer, PostMessageRequest.toStupidStreamObject(line[1], line[2]));
                    break;
                case "search":
                    String resp = httpRequestResponse(argLuceneAddress, "lucene/search", line[1]);
                    System.out.println("Search response: " + resp);
                    break;
                default:
                    System.out.println("Couldn't catch that");
            }
        }
    }
}
