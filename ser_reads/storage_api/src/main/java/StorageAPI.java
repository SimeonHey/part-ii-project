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
    }

    private byte[] receiveResponse(String query) {
        LOGGER.info(String.format("Received response %s", query));
        return null;
    }

    public void postMessage(Message message) {
        LOGGER.info("Posting message " + message);
        KafkaUtils.produceMessage(this.producer,
            this.transactionsTopic,
            new RequestPostMessage(message).toStupidStreamObject()
        );
    }

    public ResponseSearchMessage searchMessage(String searchText) throws IOException {
//        return gson.fromJson(
//            HttpUtils.httpRequestResponse(addressLucene, "search", searchText),
//            ResponseSearchMessage.class
//        );

        KafkaUtils.produceMessage(
            this.producer,
            this.transactionsTopic,
            new RequestSearchMessage(searchText, "response").toStupidStreamObject());
        return new ResponseSearchMessage();
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
