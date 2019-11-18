import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;

public class LoopingConsumer<K, V> extends SubscribableConsumer<K, V>{
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
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());

                subscribers.forEach(subscriber -> subscriber.messageReceived(record));
            });
            // commits the offset of record to broker.
            kafkaConsumer.commitAsync();
        }

        kafkaConsumer.close();
    }
}
