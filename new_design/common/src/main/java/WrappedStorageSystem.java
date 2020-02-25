public interface WrappedStorageSystem {
    // Read requests
//    ResponseMessageDetails searchAndDetails(RequestSearchAndDetails requestSearchAndDetails);
    ResponseMessageDetails getMessageDetails(RequestMessageDetails requestMessageDetails);
    ResponseAllMessages getAllMessages(RequestAllMessages requestAllMessages);
    ResponseSearchMessage searchMessage(RequestSearchMessage requestSearchMessage);

    // Write requests
    void postMessage(RequestPostMessage postMessage);
    void deleteAllMessages();
    
}
