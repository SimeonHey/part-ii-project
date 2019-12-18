import java.util.logging.Logger;

public class RequestSearchMessage {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchMessage.class.getName());
    private static final String KEY_SEARCH_TEXT = "searchText";
    private final String searchText;

    public RequestSearchMessage(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.SEARCH_MESSAGES) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        this.searchText = stupidStreamObject.getProperty(KEY_SEARCH_TEXT);
    }

    public RequestSearchMessage(String searchText) {
        this.searchText = searchText;
    }

    public static StupidStreamObject toStupidStreamObject(String searchText) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_MESSAGES)
            .setProperty(KEY_SEARCH_TEXT, searchText);
    }

    @Override
    public String toString() {
        return "SearchMessageRequest{" +
            "searchText='" + getSearchText() + '\'' +
            '}';
    }

    public String getSearchText() {
        return searchText;
    }
}
