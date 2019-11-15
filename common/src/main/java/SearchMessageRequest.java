public class SearchMessageRequest extends BaseRequest {
    private static final String KEY_SEARCH_TEXT = "searchText";
    private String searchText;

    public SearchMessageRequest(StupidStreamObject stupidStreamObject) {
        super(stupidStreamObject);

        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.SEARCH_MESSAGES) {
            throw new RuntimeException("Incorrect object type");
        }

        this.searchText = stupidStreamObject.getProperty(KEY_SEARCH_TEXT);
    }

    public static StupidStreamObject toStupidStreamObject(String searchText) {
        StupidStreamObject stupidStreamObject = new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_MESSAGES);
        stupidStreamObject.setProperty(KEY_SEARCH_TEXT, searchText);
        return stupidStreamObject;
    }

    public String getSearchText() {
        return searchText;
    }
}
