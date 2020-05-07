import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaConsumerObserver<K, V> {
    void messageReceived(ConsumerRecord<K, V> message);
}
