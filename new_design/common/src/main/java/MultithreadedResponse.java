public class MultithreadedResponse {
    private final long channelUuid;
    private final String fromStorageSystem;
    private final StupidStreamObject.ObjectType requestObjectType;
    private final String serializedResponse;

    public MultithreadedResponse(String fromStorageSystem,
                                 StupidStreamObject.ObjectType requestObjectType,
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
            "uuid=" + channelUuid +
            ", serializedResponse length =" + serializedResponse.length() +
            '}';
    }

    public StupidStreamObject.ObjectType getRequestObjectType() {
        return requestObjectType;
    }
}
