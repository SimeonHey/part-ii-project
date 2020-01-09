import java.util.logging.Logger;

public class RequestAllMessages extends RequestWithResponse {
    private static final Logger LOGGER = Logger.getLogger(RequestAllMessages.class.getName());

    RequestAllMessages(String responseEndpoint, long uuid) {
        super(responseEndpoint, uuid);
    }

    static RequestAllMessages fromStupidStreamObject(StupidStreamObject stupidStreamObject, long uuid) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.GET_ALL_MESSAGES) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        String responseEndpoint = stupidStreamObject.getProperty(KEY_RESPONSE_ENDPOINT);

        return new RequestAllMessages(responseEndpoint, uuid);
    }

    public static StupidStreamObject getStupidStreamObject(String responseEndpoint) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.GET_ALL_MESSAGES)
            .setProperty(KEY_RESPONSE_ENDPOINT, responseEndpoint);
    }
}
