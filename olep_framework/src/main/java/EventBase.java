public class EventBase {
    private final String objectType;
    private final boolean expectsResponse;

    private Addressable responseAddress;

    public EventBase(boolean expectsResponse) {
        this.objectType = this.getClass().getName();
        this.expectsResponse = expectsResponse;
    }

    public EventBase(Addressable responseAddress, boolean expectsResponse) {
        this.objectType = this.getClass().getName();
        this.responseAddress = responseAddress;
        this.expectsResponse = expectsResponse;
    }

    public void setResponseAddress(Addressable responseAddress) {
        this.responseAddress = responseAddress;
    }

    public String getEventType() {
        return objectType;
    }

    public Addressable getResponseAddress() {
        return responseAddress;
    }

    public boolean expectsResponse() {
        return expectsResponse;
    }

    @Override
    public String toString() {
        return "BaseEvent{" +
            "objectType='" + objectType + '\'' +
            ", responseAddress=" + responseAddress +
            '}';
    }
}
