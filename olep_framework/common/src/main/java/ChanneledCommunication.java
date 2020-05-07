import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class ChanneledCommunication {
    private final static Logger LOGGER = Logger.getLogger(ChanneledCommunication.class.getName());

    private final HashMap<Long, LinkedBlockingQueue<String>> responseChannels = new HashMap<>();
    private final HashMap<Long, LinkedBlockingQueue<String>> confirmationChannels = new HashMap<>();

    private HashMap<Long, LinkedBlockingQueue<String>> getHm(boolean isResponse) {
        return isResponse
            ? responseChannels
            : confirmationChannels;
    }

    private void createChannelIfAbsent(HashMap<Long, LinkedBlockingQueue<String>> hm, long uuid) {
        if (!hm.containsKey(uuid)) {
            hm.put(uuid, new LinkedBlockingQueue<>());
        }
    }
    private void createChannelIfAbsent(long uuid) {
        createChannelIfAbsent(responseChannels, uuid);
        createChannelIfAbsent(confirmationChannels, uuid);
    }

    public ChanneledResponse registerResponse(String serializedResponse) {
        ChanneledResponse response = Constants.gson.fromJson(serializedResponse, ChanneledResponse.class);

        HashMap<Long, LinkedBlockingQueue<String>> hmOfInterest = getHm(response.isResponse());

        LOGGER.info("Registering a " + (response.isResponse() ? "response" : "confirmation") + ": " + response);

        createChannelIfAbsent(hmOfInterest, response.getChannelUuid());
        hmOfInterest.get(response.getChannelUuid()).add(response.getSerializedResponse());
        return response;
    }

    public <T>T registerResponse(String serializedResponse, Class<T> typeOfResponse) {
        ChanneledResponse response = Constants.gson.fromJson(serializedResponse, ChanneledResponse.class);

        HashMap<Long, LinkedBlockingQueue<String>> hmOfInterest = getHm(response.isResponse());

        LOGGER.info("Registering a " + (response.isResponse() ? "response" : "confirmation"));
        createChannelIfAbsent(hmOfInterest, response.getChannelUuid());
        hmOfInterest.get(response.getChannelUuid()).add(response.getSerializedResponse());

        return Constants.gson.fromJson(response.getSerializedResponse(), typeOfResponse);
    }

    public String consume(long uuid, boolean isResponse) throws InterruptedException {
        var hm = getHm(isResponse);

        createChannelIfAbsent(hm, uuid);

        String response = hm
            .get(uuid)
            .poll(Constants.STORAGE_SYSTEMS_POLL_TIMEOUT, Constants.STORAGE_SYSTEMS_POLL_UNIT);

        if (response == null) {
            throw new RuntimeException("Error: Timeout while waiting for a response on channel " + uuid);
        }

        return response;
    }

    public String consumeAndDestroy(long uuid, boolean isResponse) throws InterruptedException {
        String response = consume(uuid, isResponse);
        getHm(isResponse).remove(uuid);
        LOGGER.info("Destroying the " + (isResponse ? "response" : "confirmation") +" on channel " + uuid);
        return response;
    }
}
