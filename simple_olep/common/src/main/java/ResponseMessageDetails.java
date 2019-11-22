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
}
