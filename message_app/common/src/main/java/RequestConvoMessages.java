public class RequestConvoMessages extends EventBase {
    private final String user1;
    private final String user2;

    RequestConvoMessages(Addressable addressable, String user1, String user2) {
        super(addressable, true);
        this.user1 = user1;
        this.user2 = user2;
    }

    public String getUser1() {
        return user1;
    }

    public String getUser2() { return user2; }
}
