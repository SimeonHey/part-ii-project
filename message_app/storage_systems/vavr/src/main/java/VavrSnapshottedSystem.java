import io.vavr.collection.HashMap;

public class VavrSnapshottedSystem extends SnapshottedStorageSystem<HashMap<String, Integer>> {
    private HashMap<String, Integer> defaultSnapshot = HashMap.empty();

    protected VavrSnapshottedSystem() {
        super(Integer.MAX_VALUE);
    }

    public void postMessage(String recipient) {
        Integer previousUnread = defaultSnapshot.getOrElse(recipient, 0);
        defaultSnapshot = defaultSnapshot.put(recipient, previousUnread+1); // Update the default snapshot too
    }

    public Integer getUnreadMessages(String ofUser) {
        return defaultSnapshot.getOrElse(ofUser, 0);
    }

    public void getAllMessages(RequestAllMessages requestAllMessages) {
        defaultSnapshot = defaultSnapshot.remove(requestAllMessages.getRequester());
    }
    @Override
    HashMap<String, Integer> getDefaultSnapshot() {
        return this.defaultSnapshot;
    }

    @Override
    HashMap<String, Integer> freshConcurrentSnapshot() {
        return this.defaultSnapshot;
    }

    @Override
    HashMap<String, Integer> refreshSnapshot(HashMap<String, Integer> bareSnapshot) {
        return this.defaultSnapshot;
    }

    @Override
    public void close() throws Exception {

    }
}
