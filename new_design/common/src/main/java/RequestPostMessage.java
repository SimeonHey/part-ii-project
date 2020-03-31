import java.util.logging.Logger;

public class RequestPostMessage extends BaseEvent {
    private static final Logger LOGGER = Logger.getLogger(RequestPostMessage.class.getName());

    private final Message message;

    public RequestPostMessage(Addressable responseAddress, Message message) {
        super(responseAddress, false);
        this.message = message;
    }

    public Message getMessage() {
        return this.message;
    }

    @Override
    public String toString() {
        return "RequestPostMessage{" +
            "message=" + message +
            '}';
    }
}
