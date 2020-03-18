import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return getSender().equals(message.getSender()) &&
            getMessageText().equals(message.getMessageText());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSender(), getMessageText());
    }
}
