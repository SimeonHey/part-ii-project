import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class KafkaUtils {
    private static final Logger LOGGER = Logger.getLogger(KafkaUtils.class.getName());

    private static final TimeMeasurer produceTimeMeasurer =
        new TimeMeasurer(Constants.METRIC_REGISTRY, "produce-times");

    public static Consumer<Long, EventBase> createConsumer(String consumerGroup,
                                                           String kafkaAddress,
                                                           String kafkaTopic,
                                                           Map<String, Class<? extends EventBase>> classMap) {
        LOGGER.info("Creating a consumer for consumerGroup " + consumerGroup);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Deserialization magic
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BaseEventDes.class.getName());
        props.put("classMap", classMap);

        Consumer<Long, EventBase> consumer = new KafkaConsumer<>(props);
        consumer.assign(Collections.singletonList(new TopicPartition(kafkaTopic, Constants.KAFKA_DEFAULT_PARTITION)));
        return consumer;
    }

    public static Producer<Long, EventBase> createProducer(String kafkaAddress, String clientId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BaseEventSer.class.getName());

        return new KafkaProducer<>(props);
    }

    static long produceMessage(Producer<Long, EventBase> producer,
                               String topic,
                               EventBase toSend) {
        var activeTimer = produceTimeMeasurer.startTimer();

        ProducerRecord<Long, EventBase> record = new ProducerRecord<>(topic,
            Constants.KAFKA_DEFAULT_PARTITION, null, toSend);

        try {
            RecordMetadata metadata = producer.send(record).get();
            LOGGER.info("Produced message of type " + toSend.getEventType()
                + " with Kafka offset = " + metadata.offset());

            return metadata.offset();
        }
        catch (ExecutionException | InterruptedException e) {
            LOGGER.warning("Error when producing message");
            throw new RuntimeException(e);
        } finally {
            produceTimeMeasurer.stopTimerAndPublish(activeTimer);
        }
    }
}
