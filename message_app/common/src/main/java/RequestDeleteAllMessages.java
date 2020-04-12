import java.util.logging.Logger;

public class RequestDeleteAllMessages extends BaseEvent {
    private static final Logger LOGGER = Logger.getLogger(RequestDeleteAllMessages.class.getName());

    public RequestDeleteAllMessages(Addressable responseAddress) {
        super(responseAddress, false, 1);
    }
}
