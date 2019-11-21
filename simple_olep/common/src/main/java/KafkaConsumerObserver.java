import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaConsumerObserver<K, V> {
    void messageReceived(ConsumerRecord<K, V> message);
}
