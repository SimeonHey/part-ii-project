public interface WrappedSnapshottedStorageSystem<T extends AutoCloseable> {
    // Read requests
    ResponseMessageDetails getMessageDetails(SnapshotHolder<T> snapshotHolder,
                                             RequestMessageDetails requestMessageDetails);
    ResponseAllMessages getAllMessages(SnapshotHolder<T> snapshotHolder,
                                       RequestAllMessages requestAllMessages);
    ResponseSearchMessage searchMessage(SnapshotHolder<T> snapshotHolder,
                                        RequestSearchMessage requestSearchMessage);

    // Write requests
    void postMessage(RequestPostMessage postMessage);
    void deleteAllMessages();
}
