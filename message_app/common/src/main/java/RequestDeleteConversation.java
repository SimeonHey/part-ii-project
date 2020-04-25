import java.util.logging.Logger;

public class RequestDeleteConversation extends EventBase {
    private static final Logger LOGGER = Logger.getLogger(RequestDeleteConversation.class.getName());
    private final String user1;
    private final String user2;

    public RequestDeleteConversation(Addressable responseAddress,
                                     String user1,
                                     String user2) {
        super(responseAddress, false);

        this.user1 = user1;
        this.user2 = user2;
    }

    public String getUser1() {
        return user1;
    }

    public String getUser2() {
        return user2;
    }

    @Override
    public String toString() {
        return "RequestDeleteConversation{" +
            "user1='" + user1 + '\'' +
            ", user2='" + user2 + '\'' +
            '}';
    }
}
