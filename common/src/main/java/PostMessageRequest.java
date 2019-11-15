public class PostMessageRequest extends BaseRequest {
    private static final String KEY_SENDER = "sender";
    private static final String KEY_MESSAGE_TEXT = "messageText";

    private String sender;
    private String messageText;

    public PostMessageRequest (StupidStreamObject stupidStreamObject) {
        super(stupidStreamObject);

        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.POST_MESSAGE) {
            throw new RuntimeException("Incorrect object type");
        }

        this.sender = stupidStreamObject.getProperty(KEY_SENDER);
        this.messageText = stupidStreamObject.getProperty(KEY_MESSAGE_TEXT);
    }

    public static StupidStreamObject toStupidStreamObject(String sender, String messageText) {
        StupidStreamObject stupidStreamObject = new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE);
        stupidStreamObject.setProperty(KEY_SENDER, sender);
        stupidStreamObject.setProperty(KEY_MESSAGE_TEXT, messageText);
        return stupidStreamObject;
    }

    public String getSender() {
        return sender;
    }

    public String getMessageText() {
        return messageText;
    }
}
