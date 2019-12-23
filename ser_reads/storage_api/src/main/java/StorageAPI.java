import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

public class StorageAPI {
    private static final Logger LOGGER = Logger.getLogger(StorageAPI.class.getName());

    private final Producer<Long, StupidStreamObject> producer;
    private final String addressPsql;
    private final String transactionsTopic;
    private final Gson gson;

    private final ArrayBlockingQueue<String> responsesQueue;

    StorageAPI(Gson gson,
               Producer<Long, StupidStreamObject> producer,
               HttpStorageSystem httpStorageSystem,
               String addressPsql,
               String transactionsTopic) {
        this.gson = gson;
        this.producer = producer;
        this.addressPsql = addressPsql;
        this.transactionsTopic = transactionsTopic;

        httpStorageSystem.registerHandler("response", this::receiveResponse);

        this.responsesQueue = new ArrayBlockingQueue<>(1); // Outstanding unprocessed responses
    }

    private byte[] receiveResponse(String query) {
        LOGGER.info(String.format("Received response %s", query));
        this.responsesQueue.add(query);
        return ("Received response " + query).getBytes();
    }

    public void postMessage(Message message) {
        LOGGER.info("Posting message " + message);
        KafkaUtils.produceMessage(this.producer,
            this.transactionsTopic,
            new RequestPostMessage(message).toStupidStreamObject()
        );
    }

    public ResponseSearchMessage searchMessage(String searchText) throws InterruptedException {
//        return gson.fromJson(
//            HttpUtils.httpRequestResponse(addressLucene, "search", searchText),
//            ResponseSearchMessage.class
//        );

        KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            new RequestSearchMessage(searchText, "response").toStupidStreamObject());

        String resp = this.responsesQueue.take();
        return gson.fromJson(resp, ResponseSearchMessage.class);
    }

    public ResponseMessageDetails messageDetails(Long uuid) throws IOException {
        return gson.fromJson(
            HttpUtils.httpRequestResponse(addressPsql, "messageDetails", uuid.toString()),
            ResponseMessageDetails.class
        );
    }

    public ResponseAllMessages allMessages() throws IOException {
        return gson.fromJson(
            HttpUtils.httpRequestResponse(addressPsql, "allMessages", ""),
            ResponseAllMessages.class
        );
    }

    public void deleteAllMessages() {
        KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            new StupidStreamObject(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES)
        );
    }

    @Override
    public String toString() {
        return "StorageAPI{" +
            "producer=" + producer +
            ", addressPsql='" + addressPsql + '\'' +
            ", transactionsTopic='" + transactionsTopic + '\'' +
            ", gson=" + gson +
            '}';
    }
}
