public class BaseEvent {
    private final String objectType;
    private final Addressable responseAddress;
    private final boolean expectsResponse;
    private int numberOfHops;

    public BaseEvent(Addressable responseAddress, boolean expectsResponse, int numberOfHops) {
        this.objectType = this.getClass().getName(); // TODO: Is reflection unnecessary here?
        this.responseAddress = responseAddress;
        this.expectsResponse = expectsResponse;
        this.numberOfHops = numberOfHops;
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

    public int getNumberOfHops() {
        return numberOfHops;
    }

    void setNumberOfHops(int numberOfHops) {
        this.numberOfHops = numberOfHops;
    }

    @Override
    public String toString() {
        return "BaseEvent{" +
            "objectType='" + objectType + '\'' +
            ", responseAddress=" + responseAddress +
            '}';
    }
}
