import java.util.logging.Logger;

public class RequestPostMessage extends Addressable {
    private static final Logger LOGGER = Logger.getLogger(RequestPostMessage.class.getName());

    private static final String KEY_SENDER = "sender";
    private static final String KEY_MESSAGE_TEXT = "messageText";

    private final Message message;

    public static RequestPostMessage fromStupidStreamObject(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.POST_MESSAGE) {
            LOGGER.warning("Stupid Stream Object has the incorrect object type");
            throw new RuntimeException("Incorrect object type");
        }

        Message message = new Message(stupidStreamObject.getProperty(KEY_SENDER),
            stupidStreamObject.getProperty(KEY_MESSAGE_TEXT));
        return new RequestPostMessage(message, stupidStreamObject.getResponseAddress());
    }

    public RequestPostMessage(Message message, Addressable responseEndpoint) {
        super(responseEndpoint);
        this.message = message;
    }

    public StupidStreamObject toStupidStreamObject() {
        return new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE, this)
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
