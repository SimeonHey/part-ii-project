import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.logging.Logger;

public class LoopingConsumer<K, V> extends ManualConsumer<K, V>{
    private static final Logger LOGGER = Logger.getLogger(LoopingConsumer.class.getName());
    private final long sleepMs;

    public LoopingConsumer(Consumer<K, V> kafkaConsumer, int sleepMs) {
        super(kafkaConsumer);
        this.sleepMs = sleepMs;
    }

    public LoopingConsumer(Consumer<K, V> kafkaConsumer) {
        super(kafkaConsumer);
        this.sleepMs = Constants.KAFKA_CONSUME_DELAY_MS;
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
                LOGGER.warning("Interrupt exception in consumer: " + e);
                throw new RuntimeException(e);
            }
        }
    }

    public static LoopingConsumer<Long, StupidStreamObject> fresh(String consumerGroup, String kafkaAddress) {
        return new LoopingConsumer<>(
            KafkaUtils.createConsumer(consumerGroup, kafkaAddress, Constants.KAFKA_TOPIC));
    }
}
