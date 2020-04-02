import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ChanneledCommunication {
    private final HashMap<Long, LinkedBlockingQueue<String>> channels;

    public ChanneledCommunication() {
        channels = new HashMap<>();
    }

    private void createChannelIfAbsent(long uuid) {
        if (!channels.containsKey(uuid)) {
            channels.put(uuid, new LinkedBlockingQueue<>());
        }
    }

    public ChanneledResponse registerResponse(String serializedResponse) {
        ChanneledResponse response = Constants.gson.fromJson(serializedResponse, ChanneledResponse.class);
        createChannelIfAbsent(response.getChannelUuid());
        channels.get(response.getChannelUuid()).add(response.getSerializedResponse());
        return response;
    }

    public <T>T registerResponse(String serializedResponse, Class<T> typeOfResponse) {
        ChanneledResponse response = Constants.gson.fromJson(serializedResponse, ChanneledResponse.class);
        createChannelIfAbsent(response.getChannelUuid());
        channels.get(response.getChannelUuid()).add(response.getSerializedResponse());
        return Constants.gson.fromJson(response.getSerializedResponse(), typeOfResponse);
    }

    public String consume(long uuid) throws InterruptedException {
        createChannelIfAbsent(uuid);

        String response = channels
            .get(uuid)
            .poll(Constants.STORAGE_SYSTEMS_POLL_TIMEOUT, Constants.STORAGE_SYSTEMS_POLL_UNIT);

        if (response == null) {
            throw new RuntimeException("Error: Timeout while waiting for a response on channel " + uuid);
        }

        return response;
    }

    public String consumeAndDestroy(long uuid) throws InterruptedException {
        String response = consume(uuid);
        channels.remove(uuid);
        return response;
    }
}
