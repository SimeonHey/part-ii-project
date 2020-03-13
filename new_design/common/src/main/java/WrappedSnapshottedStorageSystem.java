public interface WrappedSnapshottedStorageSystem<T extends AutoCloseable> extends AutoCloseable {
    // Read requests - require a snapshot (it might be the latest one, in which case it's not really a snaphost)
    ResponseMessageDetails getMessageDetails(T snapshot,
                                             RequestMessageDetails requestMessageDetails);
    ResponseAllMessages getAllMessages(T snapshotHolder,
                                       RequestAllMessages requestAllMessages);
    ResponseSearchMessage searchMessage(T snapshotHolder,
                                        RequestSearchMessage requestSearchMessage);

    // Write requests - they always operate on the latest non-snapshot connection
    void postMessage(RequestPostMessage postMessage);
    void deleteAllMessages();



    T getDefaultSnapshot();
    T getConcurrentSnapshot();
}
