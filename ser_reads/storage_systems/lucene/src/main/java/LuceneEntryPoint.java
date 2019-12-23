import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.Consumer;

public class LuceneEntryPoint {
    public static void main(String[] args) throws InterruptedException {
        // Consume program line arguments
        String argKafkaAddress = args[0];
        String argTransactionsTopic = args[1];
        String argServerAddress = args[2];

        // Connect to Kafka
        Consumer<Long, StupidStreamObject> kafkaConsumer = KafkaUtils.createConsumer(
            "lucene",
            argKafkaAddress,
            argTransactionsTopic);
        LoopingConsumer<Long, StupidStreamObject> loopingConsumer =
            new LoopingConsumer<>(kafkaConsumer, 100);

        // Connect to the Storage API
//        HttpUtils.discoverEndpoint(argServerAddress);

        LuceneWrapper luceneWrapper = new LuceneWrapper();
        Gson gson = new Gson();

        LuceneStorageSystem luceneStorageSystem =
            new LuceneStorageSystem(loopingConsumer, luceneWrapper,
                argServerAddress);

        luceneStorageSystem.deleteAllMessages();

        loopingConsumer.listenBlockingly();
    }
}
