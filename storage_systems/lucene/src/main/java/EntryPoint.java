import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class EntryPoint {
    public static void main(String[] args) {
        // Consume program line arguments
        Consumer<Long, String> consumer = MyKafkaConsumer.createConsumer();
        int noMessageFound = 0;

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            //print each record.
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
        // Connect to Kafka & possibly the storage api
        // Consume from Kafka topic & do things
        // Send results back to the storage api
    }
}
