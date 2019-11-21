import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class PsqlStorageSystem extends HttpStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject> {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystem.class.getName());
    
    private final PsqlWrapper psqlWrapper;
    private final Gson gson;

    PsqlStorageSystem(LoopingConsumer<Long, StupidStreamObject> consumer,
                      HttpServer httpServer,
                      PsqlWrapper psqlWrapper,
                      Gson gson) {
        super("psql", httpServer);

        this.psqlWrapper = psqlWrapper;
        this.gson = gson;

        consumer.subscribe(this);
        this.registerHandler("messageDetails", this::handleGetMessageDetails);
    }

    @Override
    public void messageReceived(ConsumerRecord<Long, StupidStreamObject> message) {
        LOGGER.info("Psql received values of type " + message.value().getObjectType().toString() + " with " +
            "properties:");
        message.value().getProperties().forEach((key, value) ->
            LOGGER.info(key + " - " + value));

        StupidStreamObject streamObject = message.value();
        Long uuid = message.offset();

        switch (streamObject.getObjectType()) {
            case POST_MESSAGE:
                this.psqlWrapper.postMessage(new RequestPostMessage(streamObject), uuid);
                break;
            case NOP:
                LOGGER.info("PSQL received a NOP request. Skipping...");
                break;
            default:
                throw new RuntimeException("Unknown stream object type");
        }
    }

    private byte[] handleGetMessageDetails(String query) {
        // TODO : Add sanitization
        RequestMessageDetails request = new RequestMessageDetails(Long.valueOf(query));

        String reqResult = this.psqlWrapper.getMessageDetails(request);
        String serialized = gson.toJson(reqResult);

        return serialized.getBytes();
    }
}
