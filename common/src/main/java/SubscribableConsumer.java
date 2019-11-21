import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class SubscribableConsumer<K, V> {
    static final long DEFAULT_BLOCK_MS = 10000;

    List<KafkaConsumerObserver<K, V>> subscribers;
    private Consumer<K, V> kafkaConsumer;
    private final long blockMs;

    SubscribableConsumer(Consumer<K, V> kafkaConsumer, long blockMs) {
        this.subscribers = new ArrayList<>();
        this.kafkaConsumer = kafkaConsumer;
        this.blockMs = blockMs;
    }

    SubscribableConsumer(Consumer<K, V> kafkaConsumer) {
        this.subscribers = new ArrayList<>();
        this.kafkaConsumer = kafkaConsumer;
        this.blockMs = DEFAULT_BLOCK_MS;
    }

    public void subscribe(KafkaConsumerObserver<K, V> subscriber) {
        this.subscribers.add(subscriber);
    }

    ConsumerRecords<K, V> consumeRecords() {
        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(java.time.Duration.ofMillis(blockMs));
        this.kafkaConsumer.commitSync();
        return consumerRecords;
    }

    void close() {
        this.kafkaConsumer.close();
    }
}
