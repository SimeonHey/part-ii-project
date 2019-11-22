public class Message {
    private final String sender;
    private final String messageText;

    public Message(String sender, String messageText) {
        this.sender = sender;
        this.messageText = messageText;
    }

    public String getSender() {
        return sender;
    }

    public String getMessageText() {
        return messageText;
    }

    @Override
    public String toString() {
        return "Message{" +
            "sender='" + sender + '\'' +
            ", messageText='" + messageText + '\'' +
            '}';
    }
}
