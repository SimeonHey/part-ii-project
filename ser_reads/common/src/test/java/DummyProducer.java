import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class DummyProducer implements Producer<Long, StupidStreamObject> {
    private DummyKafka dummyKafka = DummyKafka.getDummyKafka();

    @Override
    public Future<RecordMetadata> send(ProducerRecord<Long, StupidStreamObject> record) {
        long offset = dummyKafka.produceMessage(record.key(), record.value());

        RecordMetadata metadata =
            new RecordMetadata(DummyKafka.TOPIC_PARTITION, offset, offset,
                0, 0L, 0, 0);

        return CompletableFuture.completedFuture(metadata);

    }

    @Override
    public void initTransactions() {

    }

    @Override
    public void beginTransaction() throws ProducerFencedException {

    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {

    }

    @Override
    public void commitTransaction() throws ProducerFencedException {

    }

    @Override
    public void abortTransaction() throws ProducerFencedException {

    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<Long, StupidStreamObject> record, Callback callback) {
        return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void close(Duration timeout) {

    }
}
