import java.util.logging.Logger;

public class RequestMessageDetails extends BaseEvent {
    private final static Logger LOGGER = Logger.getLogger(RequestMessageDetails.class.getName());

    private final Long messageUUID;

    public RequestMessageDetails(Addressable responseAddress, Long messageUUID) {
        super(responseAddress, true, 1);
        this.messageUUID = messageUUID;
    }

    public Long getMessageUUID() {
        return this.messageUUID;
    }
}
