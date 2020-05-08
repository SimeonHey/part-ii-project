public class EventBase {
    private final String objectType;
    private final Addressable responseAddress;
    private final boolean expectsResponse;

    public EventBase(Addressable responseAddress, boolean expectsResponse) {
        this.objectType = this.getClass().getName();
        this.responseAddress = responseAddress;
        this.expectsResponse = expectsResponse;
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
