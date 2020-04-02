public class ChanneledResponse {
    private final long channelUuid;
    private final String fromStorageSystem;
    private final String requestObjectType;
    private final String serializedResponse;

    public ChanneledResponse(String fromStorageSystem,
                             String requestObjectType,
                             long channelUuid,
                             Object response) {
        this.fromStorageSystem = fromStorageSystem;
        this.requestObjectType = requestObjectType;

        this.channelUuid = channelUuid;
        this.serializedResponse = Constants.gson.toJson(response);
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

    @Override
    public String toString() {
        return "MultithreadedResponse{" +
            "channelUuid=" + channelUuid +
            ", fromStorageSystem='" + fromStorageSystem + '\'' +
            ", requestObjectType=" + requestObjectType +
            ", serializedResponse length='" + serializedResponse.length() + '\'' +
            '}';
    }

    public String getRequestObjectType() {
        return requestObjectType;
    }
}
