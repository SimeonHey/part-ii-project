public class BaseRequest {
    private final Long requestUUID;

    public BaseRequest(Long requestUUID) {
        this.requestUUID = requestUUID;
    }

    @Override
    public String toString() {
        return "BaseRequest{" +
            "uuid=" + requestUUID +
            '}';
    }

    public Long getRequestUUID() {
        return requestUUID;
    }
}
