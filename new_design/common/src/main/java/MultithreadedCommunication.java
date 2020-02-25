import com.google.gson.Gson;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

public class MultithreadedCommunication {
    private final HashMap<Long, ArrayBlockingQueue<String>> channels;
    private final Gson gson = new Gson();

    public MultithreadedCommunication() {
        channels = new HashMap<>();
    }

    private void createChannelIfAbsent(long uuid) {
        if (!channels.containsKey(uuid)) {
            channels.put(uuid, new ArrayBlockingQueue<>(1));
        }
    }

    public void registerResponse(String serializedResponse) {
        MultithreadedResponse response = gson.fromJson(serializedResponse, MultithreadedResponse.class);

        createChannelIfAbsent(response.getChannelUuid());

        channels.get(response.getChannelUuid()).add(response.getSerializedResponse());
    }

    public String consumeAndDestroy(long uuid) throws InterruptedException {
        createChannelIfAbsent(uuid);

        String response = channels
            .get(uuid)
            .poll(Constants.STORAGE_SYSTEMS_POLL_TIMEOUT, Constants.STORAGE_SYSTEMS_POLL_UNIT);
        // TODO: Timeout of polling doesn't work

        if (response == null) {
            throw new RuntimeException("Error: Timeout while waiting for a response on channel " + uuid);
        }

        channels.remove(uuid);
        return response;
    }
}
