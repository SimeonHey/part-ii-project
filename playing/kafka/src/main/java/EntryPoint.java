import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.reflect.internal.pickling.UnPickler;

public class EntryPoint {
    public static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        Producer<Long, String> producer = createProducer();
        Scanner inScanner = new Scanner(System.in);

        long messagesCount = 0;

        System.out.println("Sending messages for messages...");
        while (true) {
            String messageToSend = inScanner.nextLine();
            ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME,
                messagesCount, messageToSend);

            producer.send(producerRecord, (arg1, arg2) -> {
                System.out.println(String.format("Message %s sent", messageToSend));
                System.out.println(arg1);
            });
        }
    }
}
