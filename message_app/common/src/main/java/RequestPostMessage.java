import java.util.logging.Logger;

public class RequestPostMessage extends BaseEvent {
    private static final Logger LOGGER = Logger.getLogger(RequestPostMessage.class.getName());

    private final Message message;
    private final String recipient;

    public RequestPostMessage(Addressable responseAddress, Message message, String recipient) {
        super(responseAddress, false);
        this.message = message;
        this.recipient = recipient;
    }

    public Message getMessage() {
        return this.message;
    }

    public String getRecipient() {
        return recipient;
    }

    @Override
    public String toString() {
        return "RequestPostMessage{" +
            "message=" + message +
            ", recipient='" + recipient + '\'' +
            '}';
    }
}
