import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

public class ManualConsumer {
    private final static Logger LOGGER = Logger.getLogger(ManualConsumer.class.getName());

    private static final long DEFAULT_BLOCK_ON_POLL_MS = 1; // TODO: Check

    final List<KafkaConsumerObserver<Long, EventBase>> subscribers = new CopyOnWriteArrayList<>();
    protected Consumer<Long, EventBase> kafkaConsumer;

    ManualConsumer(Consumer<Long, EventBase> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    ManualConsumer(String consumerGroup,
                   String kafkaAddress,
                   String kafkaTopic,
                   Map<String, Class<? extends EventBase>> classMap) {
        this.kafkaConsumer = KafkaUtils.createConsumer(consumerGroup, kafkaAddress, kafkaTopic, classMap);
    }

    public void subscribe(KafkaConsumerObserver<Long, EventBase> subscriber) {
        this.subscribers.add(subscriber);
        LOGGER.info("Adding a subscriber for a total of " + this.subscribers.size() + " subscribers");
    }

    ConsumerRecords<Long, EventBase> consumeRecords() {
        ConsumerRecords<Long, EventBase> consumerRecords =
            this.kafkaConsumer.poll(java.time.Duration.ofMillis(DEFAULT_BLOCK_ON_POLL_MS));
        this.kafkaConsumer.commitSync();

        if (consumerRecords.count() > 0) {
            LOGGER.info("Something consumed " + consumerRecords.count() + " records");
        }

        return consumerRecords;
    }

    public void moveAllToLatest() {
        this.kafkaConsumer.poll(Duration.ofMillis(0)); // Kafka is Lazy

        LOGGER.info("Moving consumer to the latest offset");
        this.kafkaConsumer.seekToEnd(Collections.emptyList()); // All partitions
        LOGGER.info("Kafka consumer moved to offset " +
            this.kafkaConsumer.endOffsets(this.kafkaConsumer.assignment()));
    }

    public int consumeAvailableRecords() {
        ConsumerRecords<Long, EventBase> consumerRecords = this.consumeRecords();

        LOGGER.info("Consumed " + consumerRecords.count() + " records. Pinging all of the " +
            this.subscribers.size() + " subscribers...");

        consumerRecords.forEach(record -> this.subscribers.forEach(subscriber -> subscriber.messageReceived(record)));

        LOGGER.info("Successfully consumed " + consumerRecords.count() + " records.");
        return consumerRecords.count();
    }

    void close() {
        this.kafkaConsumer.close();
    }
}
