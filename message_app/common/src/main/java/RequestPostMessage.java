import java.util.logging.Logger;

public class RequestPostMessage extends BaseEvent {
    private static final Logger LOGGER = Logger.getLogger(RequestPostMessage.class.getName());

    private final Message message;
    private final String recepient;

    public RequestPostMessage(Addressable responseAddress, Message message, String recepient) {
        super(responseAddress, false, 1);
        this.message = message;
        this.recepient = recepient;
    }

    public Message getMessage() {
        return this.message;
    }

    public String getRecepient() {
        return recepient;
    }

    @Override
    public String toString() {
        return "RequestPostMessage{" +
            "message=" + message +
            '}';
    }
}
