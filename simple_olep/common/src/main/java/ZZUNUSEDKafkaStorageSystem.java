import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public abstract class ZZUNUSEDKafkaStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject> {
    private final static Logger LOGGER = Logger.getLogger(ZZUNUSEDKafkaStorageSystem.class.getName());

    public ZZUNUSEDKafkaStorageSystem(SubscribableConsumer<Long, StupidStreamObject> consumer) {
        consumer.subscribe(this);
    }

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        LOGGER.info("Lucene received values of type " + message.value().getObjectType().toString() + " with " +
            "properties:");
        message.value().getProperties().forEach((key, value) ->
            LOGGER.info(key + " - " + value));

        StupidStreamObject streamObject = message.value();
        long uuid = message.offset();
        switch (streamObject.getObjectType()) {
            case POST_MESSAGE:
                this.postMessage(new RequestPostMessage(streamObject), uuid);
                break;
            case DELETE_ALL_MESSAGES:
                this.deleteAllMessages();
            case NOP:
                LOGGER.info("Received a NOP. Skipping...");
                break;
            default:
                LOGGER.warning("Received unkown message type");
                throw new RuntimeException("Unknown stream object type");
        }
    }

    public abstract void postMessage(RequestPostMessage postMessage, long uuid);
    public abstract void deleteAllMessages();
}
