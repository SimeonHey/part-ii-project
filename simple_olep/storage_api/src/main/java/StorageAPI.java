import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.logging.Logger;

public class StorageAPI {
    private static final Logger LOGGER = Logger.getLogger(StorageAPI.class.getName());

    private final Producer<Long, StupidStreamObject> producer;
    private final String addressLucene;
    private final String addressPsql;
    private final String transactionsTopic;
    private final Gson gson;

    public StorageAPI(Gson gson,
                      Producer<Long, StupidStreamObject> producer,
                      String addressLucene,
                      String addressPsql,
                      String transactionsTopic) {
        this.gson = gson;
        this.producer = producer;
        this.addressLucene = addressLucene;
        this.addressPsql = addressPsql;
        this.transactionsTopic = transactionsTopic;
    }

    public void postMessage(Message message) {
        LOGGER.info("Posting message " + message);
        KafkaUtils.produceMessage(this.producer,
            this.transactionsTopic,
            new RequestPostMessage(message).toStupidStreamObject());
    }

    public ResponseSearchMessage searchMessage(String searchText) throws IOException {
        return gson.fromJson(
            HttpUtils.httpRequestResponse(addressLucene, "search", searchText),
            ResponseSearchMessage.class);
    }

    public ResponseMessageDetails messageDetails(Long uuid) throws IOException {
        return gson.fromJson(
            HttpUtils.httpRequestResponse(addressPsql, "messageDetails", uuid.toString()),
            ResponseMessageDetails.class);
    }

    @Override
    public String toString() {
        return "StorageAPI{" +
            "addressLucene='" + addressLucene + '\'' +
            ", addressPsql='" + addressPsql + '\'' +
            ", transactionsTopic='" + transactionsTopic + '\'' +
            '}';
    }
}
