public interface StorageSystem {
    // Read requests
    void searchAndDetails(RequestSearchAndDetails requestSearchAndDetails);
    void getMessageDetails(RequestMessageDetails requestMessageDetails);
    void getAllMessages(RequestAllMessages requestAllMessages);
    void searchMessage(RequestSearchMessage requestSearchMessage);

    // Write requests
    void postMessage(RequestPostMessage postMessage);
    void deleteAllMessages();
    
}
