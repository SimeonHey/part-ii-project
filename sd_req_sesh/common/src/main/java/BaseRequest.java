public class BaseRequest {
    private final Long uuid;

    public BaseRequest(Long uuid) {
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        return "BaseRequest{" +
            "uuid=" + uuid +
            '}';
    }

    public Long getUuid() {
        return uuid;
    }
}
