import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import java.util.Collections;
import java.util.Properties;

public class KafkaUtils {
    private static final Logger LOGGER = Logger.getLogger(KafkaUtils.class.getName());

    public static Consumer<Long, StupidStreamObject> createConsumer(String consumerGroup, String kafkaAddress,
                                                                    String kafkaTopic) {
        LOGGER.info("Creating a consumer for consumerGroup " + consumerGroup);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StupidStreamObjectDes.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Consumer<Long, StupidStreamObject> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(kafkaTopic));
        return consumer;
    }

    public static Producer<Long, StupidStreamObject> createProducer(String kafkaAddress, String clientId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StupidStreamObjectSer.class.getName());

        return new KafkaProducer<>(props);
    }

    static void produceMessage(Producer<Long, StupidStreamObject> producer,
                               String topic,
                               StupidStreamObject toSend) {
        ProducerRecord<Long, StupidStreamObject> record = new ProducerRecord<>(topic, toSend);
        try {
            RecordMetadata metadata = producer.send(record).get();
            LOGGER.info("Produced message of type " + toSend.getObjectType()
                + " with Kafka offset = " + metadata.offset());
        }
        catch (ExecutionException | InterruptedException e) {
            LOGGER.warning("Error when producing message");
            throw new RuntimeException(e);
        }
    }
}
