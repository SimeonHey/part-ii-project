import org.apache.kafka.clients.consumer.Consumer;

import java.util.ArrayList;
import java.util.List;

public class SubscribableConsumer<K, V> {
    protected List<KafkaConsumerObserver<K, V>> subscribers;
    protected Consumer<K, V> kafkaConsumer;

    protected SubscribableConsumer(Consumer<K, V> kafkaConsumer) {
        subscribers = new ArrayList<>();
        this.kafkaConsumer = kafkaConsumer;
    }

    public void subscribe(KafkaConsumerObserver<K, V> subscriber) {
        this.subscribers.add(subscriber);
    }
}
