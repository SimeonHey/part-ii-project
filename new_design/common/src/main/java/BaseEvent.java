public class BaseEvent {
    private final String objectType;
    private final Addressable responseAddress;
    private final boolean expectsResponse;

    public BaseEvent(Addressable responseAddress, boolean expectsResponse) {
        this.objectType = this.getClass().getName(); // TODO: Is reflection unnecessary here?
        this.responseAddress = responseAddress;
        this.expectsResponse = expectsResponse;
    }

    public String getObjectType() {
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
