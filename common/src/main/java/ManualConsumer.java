import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.logging.Logger;

public class ManualConsumer<K, V> extends SubscribableConsumer<K, V> {
    private static final Logger logger = Logger.getLogger(ManualConsumer.class.getName());
    
    public ManualConsumer(Consumer<K, V> kafkaConsumer) {
        super(kafkaConsumer);
    }

    public int consumeAvailableRecords() {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(1000);

        //print each record.
        consumerRecords.forEach(record -> {
            logger.info("Record Key " + record.key());
            logger.info("Record value " + record.value());
            logger.info("Record partition " + record.partition());
            logger.info("Record offset " + record.offset());

            this.subscribers.forEach(subscriber -> subscriber.messageReceived(record));
        });
        // commits the offset of record to broker.
        this.kafkaConsumer.commitAsync();

        this.kafkaConsumer.close();
        return consumerRecords.count();
    }
}
