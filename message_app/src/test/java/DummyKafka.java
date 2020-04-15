import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class DummyKafka {
    private final static Logger LOGGER = Logger.getLogger(DummyKafka.class.getName());
    static final TopicPartition TOPIC_PARTITION = new TopicPartition("dummy", 123);

    private static final DummyKafka DUMMY_KAFKA = new DummyKafka();

    private ArrayList<ConsumerRecord<Long, EventBase>> records;
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

    synchronized long produceMessage(Long key, EventBase value) {
        long offset = records.size();

        LOGGER.info("DUMMY KAFKA: producing message " + value + " with offset " + offset);

        ConsumerRecord<Long, EventBase> consumerRecord =
            new ConsumerRecord<>(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(), offset, key, value);
        records.add(consumerRecord);

        LOGGER.info("State of the log: " + records.stream().map(r ->
            r.value().getEventType()).collect(Collectors.toList()).toString());

        return records.size()-1;
    }

    synchronized ConsumerRecords<Long, EventBase> consumeMessages(String consumerGroup) {
        // TODO: Might want to make it thread safe

        int consumeFrom = getConsumeFromAndUpdateIt(consumerGroup);
        LOGGER.info("DUMMY KAFKA: " + consumerGroup + " consuming messages of length " +
            (this.records.size() - consumeFrom) + " starting from " + consumeFrom + ": " +
            records.subList(consumeFrom,
                this.records.size()).stream().map(r -> r.value().getEventType()).collect(Collectors.toList()).toString());

        return new ConsumerRecords<>(
            Collections.singletonMap(TOPIC_PARTITION, records.subList(consumeFrom, this.records.size())));
    }
}
