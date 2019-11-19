import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.logging.Logger;

public class LoopingConsumer<K, V> extends SubscribableConsumer<K, V>{
    private static final Logger LOGGER = Logger.getLogger(LoopingConsumer.class.getName());
    
    public LoopingConsumer(Consumer<K, V> kafkaConsumer) {
        super(kafkaConsumer);
    }

    public void listenBlockingly() {
        int noMessageFound = 0;

        while (true) {
            ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(1000);
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
                LOGGER.info("Record Key " + record.key());
                LOGGER.info("Record value " + record.value());
                LOGGER.info("Record partition " + record.partition());
                LOGGER.info("Record offset " + record.offset());

                subscribers.forEach(subscriber -> subscriber.messageReceived(record));
            });
            // commits the offset of record to broker.
            kafkaConsumer.commitAsync();
        }

        kafkaConsumer.close();
    }
}
