public class RequestPostMessage {
    private static final String KEY_SENDER = "sender";
    private static final String KEY_MESSAGE_TEXT = "messageText";

    private final String sender;
    private final String messageText;

    public RequestPostMessage(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.POST_MESSAGE) {
            throw new RuntimeException("Incorrect object type");
        }

        this.sender = stupidStreamObject.getProperty(KEY_SENDER);
        this.messageText = stupidStreamObject.getProperty(KEY_MESSAGE_TEXT);
    }

    public static StupidStreamObject toStupidStreamObject(String sender, String messageText) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE)
            .setProperty(KEY_SENDER, sender)
            .setProperty(KEY_MESSAGE_TEXT, messageText);
    }

    public String getSender() {
        return sender;
    }

    public String getMessageText() {
        return messageText;
    }

    @Override
    public String toString() {
        return "PostMessageRequest{" +
            "sender='" + sender + '\'' +
            ", messageText='" + messageText + '\'' +
            '}';
    }
}
