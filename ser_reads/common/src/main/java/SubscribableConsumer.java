import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class SubscribableConsumer<K, V> {
    private final static Logger LOGGER = Logger.getLogger(SubscribableConsumer.class.getName());

    static final long DEFAULT_BLOCK_MS = 10 * 1000; // TODO: Check

    List<KafkaConsumerObserver<K, V>> subscribers;
    protected Consumer<K, V> kafkaConsumer;
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
//        LOGGER.info("Consumed " + consumerRecords.count() + " records");
        return consumerRecords;
    }

    public void moveAllToLatest() {
        this.kafkaConsumer.seekToEnd(this.kafkaConsumer.assignment());
    }

    void close() {
        this.kafkaConsumer.close();
    }
}
