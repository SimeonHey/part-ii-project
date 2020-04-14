import java.util.logging.Logger;

public class RequestAllMessages extends BaseEvent {
    private static final Logger LOGGER = Logger.getLogger(RequestAllMessages.class.getName());
    private final String requester;

    RequestAllMessages(Addressable addressable, String requester) {
        super(addressable, true);
        this.requester = requester;
    }

    public String getRequester() {
        return requester;
    }
}
