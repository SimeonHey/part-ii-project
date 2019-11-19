import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import kafka.log.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

public class PsqlStorageSystem implements KafkaConsumerObserver<Long, StupidStreamObject> {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystem.class.getName());
    
    private PsqlWrapper psqlWrapper;
    private Gson gson;

    public PsqlStorageSystem(LoopingConsumer<Long, StupidStreamObject> consumer,
                             HttpServer httpServer,
                             PsqlWrapper psqlWrapper,
                             Gson gson) {
        this.psqlWrapper = psqlWrapper;
        this.gson = gson;

        consumer.subscribe(this);
        httpServer.createContext("/psql/discover", this::handleDiscover);
        httpServer.createContext("/psql/messageDetails", this::handleGetMessageDetails);
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
                this.psqlWrapper.postMessage(new PostMessageRequest(streamObject), uuid);
                break;
            default:
                throw new RuntimeException("Unknown stream object type");
        }
    }

    private void handleDiscover(HttpExchange httpExchange) throws IOException {
        httpExchange.sendResponseHeaders(200, 0);
        httpExchange.close();
    }

    private void handleGetMessageDetails(HttpExchange httpExchange) throws IOException {
        String query = httpExchange.getRequestURI().getQuery();
        // TODO : Add sanitization
        MessageDetailsRequest request = new MessageDetailsRequest(Long.valueOf(query));

        String reqResult = this.psqlWrapper.getMessageDetails(request);
        String serialized = gson.toJson(reqResult);

        httpExchange.sendResponseHeaders(200, serialized.getBytes().length);

        OutputStream os = httpExchange.getResponseBody();
        os.write(serialized.getBytes());
        os.close();

        httpExchange.close();
    }
}
