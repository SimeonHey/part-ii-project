public class SearchMessageRequest {
    private static final String KEY_SEARCH_TEXT = "searchText";
    private String searchText;

    public SearchMessageRequest(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.SEARCH_MESSAGES) {
            throw new RuntimeException("Incorrect object type");
        }

        this.searchText = stupidStreamObject.getProperty(KEY_SEARCH_TEXT);
    }

    public SearchMessageRequest(String searchText) {
        this.searchText = searchText;
    }

    public static StupidStreamObject toStupidStreamObject(String searchText) {
        StupidStreamObject stupidStreamObject = new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_MESSAGES);
        stupidStreamObject.setProperty(KEY_SEARCH_TEXT, searchText);
        return stupidStreamObject;
    }

    @Override
    public String toString() {
        return "SearchMessageRequest{" +
            "searchText='" + searchText + '\'' +
            '}';
    }

    public String getSearchText() {
        return searchText;
    }
}
