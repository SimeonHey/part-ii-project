public class RequestGetUnreadMessages extends EventBase {
    private final String ofUser;

    public RequestGetUnreadMessages(Addressable responseAddress, String ofUser) {
        super(responseAddress, true);
        this.ofUser = ofUser;
    }

    public String getOfUser() {
        return ofUser;
    }
}
