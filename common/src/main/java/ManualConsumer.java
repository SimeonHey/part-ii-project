import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ManualConsumer<K, V> extends SubscribableConsumer<K, V> {
    public ManualConsumer(Consumer<K, V> kafkaConsumer) {
        super(kafkaConsumer);
    }

    public int consumeAvailableRecords() {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(1000);

        //print each record.
        consumerRecords.forEach(record -> {
            System.out.println("Record Key " + record.key());
            System.out.println("Record value " + record.value());
            System.out.println("Record partition " + record.partition());
            System.out.println("Record offset " + record.offset()); // TODO: Could use this as uuid (mathes the order)

            this.subscribers.forEach(subscriber -> subscriber.messageReceived(record));
        });
        // commits the offset of record to broker.
        this.kafkaConsumer.commitAsync();

        this.kafkaConsumer.close();
        return consumerRecords.count();
    }
}
