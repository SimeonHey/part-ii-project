public interface MessageAppDatabase<T> {
    ResponseMessageDetails getMessageDetails(T snapshot,
                                             RequestMessageDetails requestMessageDetails);
    ResponseAllMessages getConvoMessages(T snapshotHolder,
                                         RequestConvoMessages requestConvoMessages);
    ResponseSearchMessage searchMessage(T snapshotHolder,
                                        RequestSearchMessage requestSearchMessage);

    // Write requests - they always operate on the latest non-snapshot connection
    void postMessage(RequestPostMessage postMessage);
    void deleteConvoThread(RequestDeleteConversation deleteConversation);
    void deleteAllMessages();
}
