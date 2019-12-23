import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

public class MultithreadedCommunication {
    private final HashMap<Long, ArrayBlockingQueue<String>> channels;

    public MultithreadedCommunication() {
        channels = new HashMap<>();
    }

    private void createChannelIfAbsent(long uuid) {
        if (!channels.containsKey(uuid)) {
            channels.put(uuid, new ArrayBlockingQueue<>(1));
        }
    }

    public void registerResponse(MultithreadedResponse response) {
        createChannelIfAbsent(response.getUuid());

        channels.get(response.getUuid()).add(response.getSerializedResponse());
    }

    public String consumeAndDestroy(long uuid) throws InterruptedException {
        createChannelIfAbsent(uuid);

        String response = channels.get(uuid).take();
        channels.remove(uuid);
        return response;
    }
}
