public class MessageDetailsRequest {
    private Long uuid;

    public MessageDetailsRequest(Long uuid) {
        this.uuid = uuid;
    }

    public Long getUuid() {
        return this.uuid;
    }
}
