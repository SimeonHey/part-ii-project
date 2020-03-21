import java.util.logging.Logger;

public class RequestMessageDetails extends BaseRequest {
    private final static String KEY_MESSAGE_UUID = "messageUUID";

    private final static Logger LOGGER = Logger.getLogger(RequestMessageDetails.class.getName());

    private final Long messageUUID;

    public RequestMessageDetails(Long messageUUID) {
        super(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS);
        this.messageUUID = messageUUID;
    }

    public RequestMessageDetails(StupidStreamObject serializedRequest) {
        super(serializedRequest);

        if (serializedRequest.getObjectType() != StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS) {
            LOGGER.warning("Stupid Stream Object has the incorrect object type");
            throw new RuntimeException("Incorrect object type");
        }

        this.messageUUID = Long.valueOf(serializedRequest.getProperty(KEY_MESSAGE_UUID));
    }

    public static StupidStreamObject getStupidStreamObject(Long messageUUID, Addressable responseAddress) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, responseAddress)
            .setProperty(KEY_MESSAGE_UUID, String.valueOf(messageUUID));
    }

    @Override
    public StupidStreamObject toStupidStreamObject(Addressable responseAddress) {
        return getStupidStreamObject(messageUUID, responseAddress);
    }

    public Long getMessageUUID() {
        return this.messageUUID;
    }
}
