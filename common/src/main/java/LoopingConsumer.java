import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.logging.Logger;

public class LoopingConsumer<K, V> extends SubscribableConsumer<K, V>{
    private static final Logger LOGGER = Logger.getLogger(LoopingConsumer.class.getName());
    private final long sleepMs;

    public LoopingConsumer(Consumer<K, V> kafkaConsumer, int sleepMs) {
        super(kafkaConsumer, sleepMs);
        this.sleepMs = sleepMs;
    }

    public LoopingConsumer(Consumer<K, V> kafkaConsumer) {
        super(kafkaConsumer);
        this.sleepMs = DEFAULT_BLOCK_MS;
    }

    public void listenBlockingly() {
        LOGGER.info("Indefinitely listening for Kafka messages...");

        while (true) {
            ConsumerRecords<K, V> consumerRecords = this.consumeRecords();
            consumerRecords.forEach(record ->
                subscribers.forEach(subscriber -> subscriber.messageReceived(record)));

            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                this.close();
                throw new RuntimeException(e);
            }
        }
    }
}
