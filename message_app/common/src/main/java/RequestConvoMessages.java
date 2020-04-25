import java.util.logging.Logger;

public class RequestConvoMessages extends EventBase {
    private static final Logger LOGGER = Logger.getLogger(RequestConvoMessages.class.getName());
    private final String requester;
    private final String withUser;

    RequestConvoMessages(Addressable addressable, String requester, String withUser) {
        super(addressable, true);
        this.requester = requester;
        this.withUser = withUser;
    }

    public String getRequester() {
        return requester;
    }

    public String getWithUser() {
        return withUser;
    }
}
