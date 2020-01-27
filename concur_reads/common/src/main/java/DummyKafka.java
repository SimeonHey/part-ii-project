import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Logger;

class DummyKafka {
    private final static Logger LOGGER = Logger.getLogger(DummyKafka.class.getName());
    static final TopicPartition TOPIC_PARTITION = new TopicPartition("dummy", 123);

    private static final DummyKafka DUMMY_KAFKA = new DummyKafka();

    private ArrayList<ConsumerRecord<Long, StupidStreamObject>> records;
    private HashMap<String, Integer> consumeStartGroup;

    private DummyKafka() {
        records = new ArrayList<>();
        consumeStartGroup = new HashMap<>();
    }

    private Integer getConsumeFromAndUpdateIt(String consumerGroup) {
        int old = consumeStartGroup.getOrDefault(consumerGroup, 0);
        consumeStartGroup.put(consumerGroup, records.size());
        return old;
    }

    static DummyKafka getDummyKafka() {
        return DUMMY_KAFKA;
    }

    long produceMessage(Long key, StupidStreamObject value) {
        long offset = records.size();

        LOGGER.info("DUMMY KAFKA: producing message " + value + " with offset " + offset);

        ConsumerRecord<Long, StupidStreamObject> consumerRecord =
            new ConsumerRecord<>(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(), offset, key, value);
        records.add(consumerRecord);

        return records.size()-1;
    }

    ConsumerRecords<Long, StupidStreamObject> consumeMessages(String consumerGroup) {
        // TODO: Might want to make it thread safe

        int consumeFrom = getConsumeFromAndUpdateIt(consumerGroup);
        LOGGER.info("DUMMY KAFKA: " + consumerGroup + " consuming messages of length " +
            (this.records.size() - consumeFrom) + " starting from " + consumeFrom);

        return new ConsumerRecords<>(
            Collections.singletonMap(TOPIC_PARTITION, records.subList(consumeFrom, this.records.size())));
    }
}
