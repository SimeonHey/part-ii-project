import java.util.Objects;

public class Message {
    private final String sender;
    private final String messageText;
    private final long timestamp;

    public Message(String sender, String messageText, long timestamp) {
        this.sender = sender;
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

    @Override
    public String toString() {
        return "Message{" +
            "sender='" + sender + '\'' +
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
            getMessageText().equals(message.getMessageText());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSender(), getMessageText(), getTimestamp());
    }
}
