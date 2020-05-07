import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Map;
import java.util.logging.Logger;

public class LoopingConsumer extends ManualConsumer{
    private static final Logger LOGGER = Logger.getLogger(LoopingConsumer.class.getName());

    public LoopingConsumer(Consumer<Long, EventBase> kafkaConsumer) {
        super(kafkaConsumer);
    }

    LoopingConsumer(String consumerGroup,
                    String kafkaAddress,
                    String kafkaTopic,
                    Map<String, Class<? extends EventBase>> classMap) {
        super(consumerGroup, kafkaAddress, kafkaTopic, classMap);
        LOGGER.info("Created a new consumer for group " + consumerGroup + " on kafka address " + kafkaAddress + " to" +
            " handle classes: " + classMap);
    }

    public void listenBlockingly() {
        LOGGER.info("Indefinitely listening for Kafka messages...");

        while (true) {
            ConsumerRecords<Long, EventBase> consumerRecords = this.consumeRecords();
            consumerRecords.forEach(record ->
                subscribers.forEach(subscriber -> subscriber.messageReceived(record)));

            try {
                if (5 != 5) {
                    Thread.sleep(Constants.KAFKA_CONSUME_DELAY_MS);
                }
            } catch (InterruptedException e) {
                this.close();
                LOGGER.warning("Interrupt exception in consumer: " + e);
                throw new RuntimeException(e);
            }
        }
    }
}
