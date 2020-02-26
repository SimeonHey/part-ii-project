import java.util.logging.Logger;

public class RequestPostMessage extends BaseRequest{
    private static final Logger LOGGER = Logger.getLogger(RequestPostMessage.class.getName());

    private static final String KEY_SENDER = "sender";
    private static final String KEY_MESSAGE_TEXT = "messageText";

    private final Message message;

    public static RequestPostMessage fromStupidStreamObject(StupidStreamObject stupidStreamObject, long uuid) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.POST_MESSAGE) {
            LOGGER.warning("Stupid Stream Object has the incorrect object type");
            throw new RuntimeException("Incorrect object type");
        }

        Message message = new Message(stupidStreamObject.getProperty(KEY_SENDER),
            stupidStreamObject.getProperty(KEY_MESSAGE_TEXT));
        return new RequestPostMessage(message, uuid);
    }

    public RequestPostMessage(Message message, long uuid) {
        super(uuid);
        this.message = message;
    }

    public StupidStreamObject toStupidStreamObject() {
        return new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE, responseAddress)
            .setProperty(KEY_SENDER, getMessage().getSender())
            .setProperty(KEY_MESSAGE_TEXT, getMessage().getMessageText());
    }

    public Message getMessage() {
        return this.message;
    }

    @Override
    public String toString() {
        return "RequestPostMessage{" +
            "message=" + message +
            '}';
    }
}
