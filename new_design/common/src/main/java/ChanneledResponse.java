public class ChanneledResponse {
    private final long channelUuid;
    private final String fromStorageSystem;
    private final String requestObjectType;
    private final String serializedResponse;
    private final boolean isResponse;

    public ChanneledResponse(String fromStorageSystem,
                             String requestObjectType,
                             long channelUuid,
                             Object response,
                             boolean isResponse) {
        this.fromStorageSystem = fromStorageSystem;
        this.requestObjectType = requestObjectType;

        this.channelUuid = channelUuid;
        this.serializedResponse = Constants.gson.toJson(response);

        this.isResponse = isResponse;
    }

    public String getFromStorageSystem() {
        return fromStorageSystem;
    }

    public long getChannelUuid() {
        return channelUuid;
    }

    public String getSerializedResponse() {
        return serializedResponse;
    }

    public boolean isResponse() {
        return isResponse;
    }

    @Override
    public String toString() {
        return "ChanneledResponse{" +
            "channelUuid=" + channelUuid +
            ", fromStorageSystem='" + fromStorageSystem + '\'' +
            ", requestObjectType='" + requestObjectType + '\'' +
            ", serializedResponse='" + serializedResponse + '\'' +
            ", isResponse=" + isResponse +
            '}';
    }

    public String getRequestObjectType() {
        return requestObjectType;
    }
}
