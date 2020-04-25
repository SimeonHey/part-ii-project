import java.util.Objects;

public class Message {
    private final String sender;
    private final String recipient;
    private final String messageText;
    private final long timestamp;

    public Message(String sender, String recipient, String messageText, long timestamp) {
        this.sender = sender;
        this.recipient = recipient;
        this.messageText = messageText;
        this.timestamp = timestamp;
    }

    public String getSender() {
        return sender;
    }

    public String getMessageText() {
        return messageText;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getRecipient() {
        return recipient;
    }

    @Override
    public String toString() {
        return "Message{" +
            "sender='" + sender + '\'' +
            ", recipient='" + recipient + '\'' +
            ", messageText='" + messageText + '\'' +
            ", timestamp=" + timestamp +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return getTimestamp() == message.getTimestamp() &&
            getSender().equals(message.getSender()) &&
            getRecipient().equals(message.getRecipient()) &&
            getMessageText().equals(message.getMessageText());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSender(), getRecipient(), getMessageText(), getTimestamp());
    }
}
