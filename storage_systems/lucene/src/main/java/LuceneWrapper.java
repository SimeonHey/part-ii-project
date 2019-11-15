import org.apache.kafka.clients.consumer.ConsumerRecord;

public class LuceneWrapper implements KafkaConsumerObserver<Long, StupidStreamObject> {

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        System.out.println("Lucene received values of type " + message.value().getObjectType().toString() + " with " +
            "properties:");
        message.value().getProperties().forEach((key, value) ->
            System.out.println(key + " - " + value));
    }

    public LuceneWrapper(SubscribableConsumer<Long, StupidStreamObject> consumer) {
        consumer.subscribe(this);
    }
}
