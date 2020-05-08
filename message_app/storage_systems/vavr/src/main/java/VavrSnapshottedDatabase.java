import io.vavr.Tuple2;
import io.vavr.collection.HashMap;

import java.io.IOException;

public class VavrSnapshottedDatabase extends SnapshottedDatabase<HashMap<Tuple2<String, String>, Integer>> {
	private HashMap<Tuple2<String, String>, Integer> mainDataView = HashMap.empty();

	VavrSnapshottedDatabase() {
		super(Integer.MAX_VALUE); // The limit of concurrently-opened snapshots
	}

    // Helper methods
	private Tuple2<String, String> getPair(String user1, String user2) {
		return (user1.compareTo(user2) < 0) ? new Tuple2<>(user1, user2) : new Tuple2<>(user2, user1);
	}

	Integer getTotalMessages(HashMap<Tuple2<String, String>, Integer> snapshot, String user1, String user2) {
		return snapshot.getOrElse(getPair(user1, user2), 0);
	}

	void increaseMessagesForPair(String user1, String user2) {
		var pair = getPair(user1, user2);

		// Updates are always applied to the main data view
		Integer previousCount = getTotalMessages(mainDataView, user1, user2);
		mainDataView = mainDataView.put(pair, previousCount + 1);
	}

	void deleteConversation(String user1, String user2) {
		mainDataView = mainDataView.remove(getPair(user1, user2));
	}

	void deleteAllMessages() {
		mainDataView = HashMap.empty();
	}

    // Overrided SnapshottedDatabase interface methods
	@Override
	HashMap<Tuple2<String, String>, Integer> getMainDataView() {
		return this.mainDataView;
	}

	@Override
	HashMap<Tuple2<String, String>, Integer> freshConcurrentSnapshot() {
		return this.mainDataView;
	}

	@Override
	HashMap<Tuple2<String, String>, Integer> refreshSnapshot(HashMap<Tuple2<String, String>, Integer> bareSnapshot) {
		return this.mainDataView; // Refreshing a snapshot is not possible or useful with the persistent HashMap
	}

	@Override
	public void close() {
		this.deleteAllMessages();
	}

	public static void main(String[] args) throws IOException {
		VavrSnapshottedDatabase wrapper = new VavrSnapshottedDatabase();

		String[] kafkaAddressTopic = {"localhost", "event_log"};
		new StorageSystemBuilder<>("VAVR", wrapper, 2233, kafkaAddressTopic)
			.registerAction(new ActionBase<>(RequestPostMessage.class, false) {
			@Override
			Response handleEvent(EventBase request, StorageSystem<HashMap<Tuple2<String, String>, Integer>> self, HashMap<Tuple2<String, String>, Integer> snapshot) {

				var postRequest = (RequestPostMessage) request;
				wrapper.increaseMessagesForPair(postRequest.getMessage().getSender(), postRequest.getMessage().getRecipient());

				return Response.CONFIRMATION;
			}
		})
			.registerAction(new ActionBase<>(RequestDeleteConversation.class, false) {
			@Override
			Response handleEvent(EventBase request, StorageSystem<HashMap<Tuple2<String, String>, Integer>> self, HashMap<Tuple2<String, String>, Integer> snapshot) {

				var deleteRequest = (RequestDeleteConversation) request;
				wrapper.deleteConversation(deleteRequest.getUser1(), deleteRequest.getUser2());

				return Response.CONFIRMATION;
			}
		})
			.registerAction(new ActionBase<>(RequestDeleteAllMessages.class, false) {
			@Override
			Response handleEvent(EventBase request, StorageSystem<HashMap<Tuple2<String, String>, Integer>> self, HashMap<Tuple2<String, String>, Integer> snapshot) {

				wrapper.deleteAllMessages();

				return Response.CONFIRMATION;
			}
		})
			.registerAction(new ActionBase<>(RequestGetTotalNumberOfMessages.class, true) {
			@Override
			Response handleEvent(EventBase request, StorageSystem<HashMap<Tuple2<String, String>, Integer>> self, HashMap<Tuple2<String, String>, Integer> snapshot) {

				var numberRequest = (RequestGetTotalNumberOfMessages) request;

				return new Response(wrapper.getTotalMessages(snapshot, numberRequest.getOfUser1(), numberRequest.getOfUser2()));
			}
		}).buildAndRun();
	}
}
