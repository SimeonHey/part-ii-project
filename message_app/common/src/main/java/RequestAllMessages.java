import java.util.logging.Logger;

public class RequestAllMessages extends BaseEvent {
    private static final Logger LOGGER = Logger.getLogger(RequestAllMessages.class.getName());

    RequestAllMessages(Addressable addressable) {
        super(addressable, true, 1);
    }
}
