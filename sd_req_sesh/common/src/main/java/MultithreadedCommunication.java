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

        String response = channels.get(uuid).take();
        channels.remove(uuid);
        return response;
    }
}
