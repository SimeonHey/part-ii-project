import java.util.Objects;

public class ResponseMessageDetails {
    private final Message message;
    private final long uuid;

    public ResponseMessageDetails(Message message, long uuid) {
        this.message = message;
        this.uuid = uuid;
    }

    public Message getMessage() {
        return message;
    }

    public long getUuid() {
        return uuid;
    }

    @Override
    public String toString() {
        return "ResponseMessageDetails{" +
            "message=" + message +
            ", uuid=" + uuid +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResponseMessageDetails that = (ResponseMessageDetails) o;
        return getUuid() == that.getUuid() &&
            getMessage().equals(that.getMessage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMessage(), getUuid());
    }

    StupidStreamObject toStupidStreamObject() {
        return new StupidStreamObject(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, Constants.NO_RESPONSE)
            .setProperty("messageSender", message.getSender())
            .setProperty("messageText", message.getMessageText())
            .setProperty("uuid", String.valueOf(uuid));
    }
}
