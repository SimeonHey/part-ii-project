import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

public class MessageAppEntryPoint {
    public static void main(String[] args) {
        String kafkaAddress = "localhost:9092";
        String luceneAddress = "localhost:8001";
        String psqlAddress = "localhost:8002";
        String transactionsTopic = "transactions_app";

        Gson gson = new Gson();
        Producer<Long, StupidStreamObject> producer =
            KafkaUtils.createProducer(kafkaAddress, "message_app");
        StorageAPI storageAPI =
            new StorageAPI(gson, producer, luceneAddress, psqlAddress, transactionsTopic);


    }
}

