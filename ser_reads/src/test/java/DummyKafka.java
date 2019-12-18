import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;

class DummyKafka {
    private static final DummyKafka DUMMY_KAFKA = new DummyKafka();

    static final TopicPartition TOPIC_PARTITION = new TopicPartition("dummy", 123);
    private ArrayList<ConsumerRecord<Long, StupidStreamObject>> records;

    private DummyKafka() {
        records = new ArrayList<>();
    }

    static DummyKafka getDummyKafka() {
        return DUMMY_KAFKA;
    }

    long produceMessage(Long key, StupidStreamObject value) {
        ConsumerRecord<Long, StupidStreamObject> consumerRecord =
            new ConsumerRecord<>(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(),
                records.size(), key, value);
        records.add(consumerRecord);

        return records.size()-1;
    }

    ConsumerRecords<Long, StupidStreamObject> consumeMessages() {
        // TODO: Might want to delete the messages
        return new ConsumerRecords<>(Collections.singletonMap(TOPIC_PARTITION, records));
    }
}
