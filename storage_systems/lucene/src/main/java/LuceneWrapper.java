public class LuceneWrapper {
    // Proxy for Lucene transactions

    public void postMessage(PostMessageRequest postMessageRequest) {

    }

    public SearchMessageResponse searchMessage(SearchMessageRequest searchMessageRequest) {
        return new SearchMessageResponse("The request was " +
            searchMessageRequest.getSearchText() + " but too lazy");
    }
}
