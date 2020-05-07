public class RequestGetTotalNumberOfMessages extends EventBase {
    private final String ofUser1;
    private final String ofUser2;

    public RequestGetTotalNumberOfMessages(Addressable responseAddress, String ofUser1, String ofUser2) {
        super(responseAddress, true);
        this.ofUser1 = ofUser1;
        this.ofUser2 = ofUser2;
    }

    public String getOfUser1() {
        return ofUser1;
    }
    public String getOfUser2() {
        return ofUser2;
    }
}
