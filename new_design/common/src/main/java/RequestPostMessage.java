import java.util.logging.Logger;

public class RequestPostMessage extends BaseRequest {
    private static final Logger LOGGER = Logger.getLogger(RequestPostMessage.class.getName());

    private static final String KEY_SENDER = "sender";
    private static final String KEY_MESSAGE_TEXT = "messageText";

    private final Message message;
    private final Long channelID;

    public RequestPostMessage(Message message) {
        super(StupidStreamObject.ObjectType.POST_MESSAGE);
        this.message = message;
        this.channelID = null;
    }
    
    public RequestPostMessage(StupidStreamObject serializedRequest) {
        super(serializedRequest);
        
        if (serializedRequest.getObjectType() != StupidStreamObject.ObjectType.POST_MESSAGE) {
            LOGGER.warning("Stupid Stream Object has the incorrect object type");
            throw new RuntimeException("Incorrect object type");
        }

        this.message = new Message(serializedRequest.getProperty(KEY_SENDER),
            serializedRequest.getProperty(KEY_MESSAGE_TEXT));
        this.channelID = serializedRequest.getResponseAddress().getChannelID();
    }

    @Override
    public StupidStreamObject toStupidStreamObject(Addressable responseAddress) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE, responseAddress)
            .setProperty(KEY_SENDER, getMessage().getSender())
            .setProperty(KEY_MESSAGE_TEXT, getMessage().getMessageText());
    }

    public Message getMessage() {
        return this.message;
    }

    public Long getChannelID() {
        return channelID;
    }

    @Override
    public String toString() {
        return "RequestPostMessage{" +
            "message=" + message +
            '}';
    }
}
