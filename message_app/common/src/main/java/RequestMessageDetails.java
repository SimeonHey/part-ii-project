import java.util.logging.Logger;

public class RequestMessageDetails extends EventBase {
    private final static Logger LOGGER = Logger.getLogger(RequestMessageDetails.class.getName());

    private final Long messageUUID;

    public RequestMessageDetails(Addressable responseAddress, Long messageUUID) {
        super(responseAddress, true);
        this.messageUUID = messageUUID;
    }

    public Long getMessageID() {
        return this.messageUUID;
    }
}
