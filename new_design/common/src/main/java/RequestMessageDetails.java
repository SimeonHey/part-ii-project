import java.util.logging.Logger;

public class RequestMessageDetails extends RequestWithResponse {
    private final static String KEY_MESSAGE_UUID = "messageUUID";

    private final static Logger LOGGER = Logger.getLogger(RequestMessageDetails.class.getName());

    private final Long messageUUID;

    public RequestMessageDetails(Long messageUUID, String responseEndpoint, long requestUUID) {
        super(responseEndpoint, requestUUID);
        this.messageUUID = messageUUID;
    }

    static RequestMessageDetails fromStupidStreamObject(StupidStreamObject stupidStreamObject, long uuid) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS) {
            LOGGER.warning("Stupid Stream Object has the incorrect object type");
            throw new RuntimeException("Incorrect object type");
        }

        Long messageUUID = Long.valueOf(stupidStreamObject.getProperty(KEY_MESSAGE_UUID));
        String responseEndpoint = stupidStreamObject.getProperty(KEY_RESPONSE_ENDPOINT);

        return new RequestMessageDetails(messageUUID, responseEndpoint, uuid);
    }

    public static StupidStreamObject getStupidStreamObject(Long messageUUID, String responseEndpoint) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, responseEndpoint)
            .setProperty(KEY_MESSAGE_UUID, String.valueOf(messageUUID))
            .setProperty(KEY_RESPONSE_ENDPOINT, responseEndpoint);
    }

    public Long getMessageUUID() {
        return this.messageUUID;
    }
}
