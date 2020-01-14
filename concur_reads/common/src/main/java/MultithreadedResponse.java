import com.google.gson.Gson;

public class MultithreadedResponse {
    private final long channelUuid;
    private final String serializedResponse;

    public MultithreadedResponse(long channelUuid, Object response) {
        this.channelUuid = channelUuid;
        Gson gson = new Gson();
        this.serializedResponse = gson.toJson(response);
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
            ", serializedResponse=" + serializedResponse +
            '}';
    }
}
