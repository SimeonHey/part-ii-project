import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.logging.Logger;

public class StorageAPI {
    private static final Logger LOGGER = Logger.getLogger(StorageAPI.class.getName());

    private final Producer<Long, StupidStreamObject> producer;
    private final String addressPsql;
    private final String transactionsTopic;
    private final Gson gson;

    private final MultithreadedCommunication multithreadedCommunication;

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

        this.multithreadedCommunication = new MultithreadedCommunication();
    }

    public void postMessage(Message message) {
        LOGGER.info("Posting message " + message);
        KafkaUtils.produceMessage(this.producer,
            this.transactionsTopic,
            new RequestPostMessage(message, -1).toStupidStreamObject()
        );
    }

    private byte[] receiveResponse(String serializedResponse) {
        LOGGER.info(String.format("Received response %s", serializedResponse));

        this.multithreadedCommunication.registerResponse(serializedResponse);
        return ("Received response " + serializedResponse).getBytes();
    }

    public ResponseSearchMessage searchMessage(String searchText) throws InterruptedException {
        long offset = KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            new RequestSearchMessage(searchText, "response", -1).toStupidStreamObject());

        LOGGER.info("Waiting for search response on channel with uuid " + offset);

        // Will block until a response is received
        String serializedResponse = this.multithreadedCommunication.consumeAndDestroy(offset);
        return gson.fromJson(serializedResponse, ResponseSearchMessage.class);
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
