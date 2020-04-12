public class RequestGetUnreadMessages extends BaseEvent {
    private final String ofUser;

    public RequestGetUnreadMessages(Addressable responseAddress, String ofUser) {
        super(responseAddress, true, 1);
        this.ofUser = ofUser;
    }

    public String getOfUser() {
        return ofUser;
    }
}
