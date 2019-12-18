import java.util.logging.Logger;

public class RequestPostMessage {
    private static final Logger LOGGER = Logger.getLogger(RequestPostMessage.class.getName());

    private static final String KEY_SENDER = "sender";
    private static final String KEY_MESSAGE_TEXT = "messageText";

    private final Message message;

    public RequestPostMessage(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.POST_MESSAGE) {
            LOGGER.warning("Stupid Stream Object has the incorrect object type");
            throw new RuntimeException("Incorrect object type");
        }

        this.message = new Message(stupidStreamObject.getProperty(KEY_SENDER),
            stupidStreamObject.getProperty(KEY_MESSAGE_TEXT));
    }

    public RequestPostMessage(Message message) {
        this.message = message;
    }

    public StupidStreamObject toStupidStreamObject() {
        return new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE)
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
