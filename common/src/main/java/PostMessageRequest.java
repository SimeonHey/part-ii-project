public class PostMessageRequest {
    private static final String KEY_SENDER = "sender";
    private static final String KEY_MESSAGE_TEXT = "messageText";
    private static final String KEY_UUID = "uuid";

    private final String sender;
    private final String messageText;
    private final Long uuid;

    public PostMessageRequest (StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.POST_MESSAGE) {
            throw new RuntimeException("Incorrect object type");
        }

        this.sender = stupidStreamObject.getProperty(KEY_SENDER);
        this.messageText = stupidStreamObject.getProperty(KEY_MESSAGE_TEXT);
        this.uuid = Long.parseLong(stupidStreamObject.getProperty(KEY_UUID));
    }

    public static StupidStreamObject toStupidStreamObject(String sender, String messageText, Long uuid) {
        StupidStreamObject stupidStreamObject = new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE);
        stupidStreamObject.setProperty(KEY_SENDER, sender);
        stupidStreamObject.setProperty(KEY_MESSAGE_TEXT, messageText);
        stupidStreamObject.setProperty(KEY_UUID, String.valueOf(uuid));
        return stupidStreamObject;
    }

    public Long getUuid() {
        return uuid;
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
            ", uuid=" + uuid +
            '}';
    }
}
