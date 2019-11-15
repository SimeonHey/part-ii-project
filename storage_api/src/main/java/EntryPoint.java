import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

public class EntryPoint {
    public static void main(String[] args) {
        // Consume command line arguments
        // Setup connection with Kafka, possibly storage systems too

        // Start listening for queries
        // Produce queries to Kafka topics
        // Possibly listen for answers

        Producer<Long, StupidStreamObject> producer = KafkaUtils.createProducer();

        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            StupidStreamObject toSend = (index%2 == 0)
                ? PostMessageRequest.toStupidStreamObject("simeon", "what's up")
                : SearchMessageRequest.toStupidStreamObject("searchtext bby");

            ProducerRecord<Long, StupidStreamObject> record = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, toSend);
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                    + " with offset " + metadata.offset());
            }
            catch (ExecutionException | InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }
}
