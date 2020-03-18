import java.util.logging.Logger;

public class RequestMessageDetails extends Addressable {
    private final static String KEY_MESSAGE_UUID = "messageUUID";

    private final static Logger LOGGER = Logger.getLogger(RequestMessageDetails.class.getName());

    private final Long messageUUID;

    public RequestMessageDetails(Long messageUUID, Addressable addressable) {
        super(addressable.getInternetAddress(), addressable.getChannelID());
        this.messageUUID = messageUUID;
    }

    static RequestMessageDetails fromStupidStreamObject(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS) {
            LOGGER.warning("Stupid Stream Object has the incorrect object type");
            throw new RuntimeException("Incorrect object type");
        }

        Long messageUUID = Long.valueOf(stupidStreamObject.getProperty(KEY_MESSAGE_UUID));

        return new RequestMessageDetails(messageUUID, stupidStreamObject.getResponseAddress());
    }

    public static StupidStreamObject getStupidStreamObject(Long messageUUID, Addressable responseAddress) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, responseAddress)
            .setProperty(KEY_MESSAGE_UUID, String.valueOf(messageUUID));
    }

    public Long getMessageUUID() {
        return this.messageUUID;
    }
}
