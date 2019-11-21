import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.logging.Logger;

public class StorageAPIEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIEntryPoint.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {
        // Consume command line arguments
        String argLuceneAddress = args[0];
        String argPsqlAddress = args[1];
        String argKafkaAddress = args[2];
        String argTransactionsTopic = args[3];

        LOGGER.info("Starting StorageAPI with params " + Arrays.toString(args));

        // Setup connections
        LOGGER.info("Initializing a Kafka producer...");
        Producer<Long, StupidStreamObject> producer =
            KafkaUtils.createProducer(argKafkaAddress, "storageAPI");
        KafkaUtils.produceMessage(producer, argTransactionsTopic, RequestNOP.toStupidStreamObject());
        LOGGER.info("Success");

        LOGGER.info("Connecting to Lucene...");
        HttpUtils.discoverEndpoint(argLuceneAddress);
        LOGGER.info("Success");

        LOGGER.info("Connecting to PSQL...");
        HttpUtils.discoverEndpoint(argPsqlAddress);
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
                    KafkaUtils.produceMessage(producer,
                        argTransactionsTopic,
                        RequestPostMessage.toStupidStreamObject(line[1], line[2]));
                    break;
                case "search":
                    String resp1 = HttpUtils.httpRequestResponse(argLuceneAddress, "search", line[1]);
                    System.out.println("Search response: " + resp1);
                    break;
                case "details":
                    String resp2 = HttpUtils.httpRequestResponse(argPsqlAddress, "messageDetails", line[1]);
                    System.out.println("Message details: " + resp2);
                    break;
                default:
                    System.out.println("Couldn't catch that");
            }
        }
    }
}
