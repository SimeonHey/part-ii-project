import com.google.gson.Gson;

public class MultithreadedResponse {
    private final long uuid;
    private final String serializedResponse;

    public MultithreadedResponse(long uuid, Object response) {
        this.uuid = uuid;
        Gson gson = new Gson();
        this.serializedResponse = gson.toJson(response);
    }

    public long getUuid() {
        return uuid;
    }

    public String getSerializedResponse() {
        return serializedResponse;
    }

    @Override
    public String toString() {
        return "MultithreadedResponse{" +
            "uuid=" + uuid +
            ", serializedResponse=" + serializedResponse +
            '}';
    }
}
