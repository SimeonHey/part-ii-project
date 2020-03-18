import java.util.logging.Logger;

public class RequestSearchMessage extends Addressable {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchMessage.class.getName());
    private static final String KEY_SEARCH_TEXT = "searchText";

    private final String searchText;

    public static RequestSearchMessage fromStupidStreamObject(StupidStreamObject stupidStreamObject) {
        // Either one of Search or S&D is fine
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.SEARCH_MESSAGES &&
            stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.SEARCH_AND_DETAILS) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        String searchText = stupidStreamObject.getProperty(KEY_SEARCH_TEXT);
        return new RequestSearchMessage(searchText, stupidStreamObject.getResponseAddress());
    }

    public RequestSearchMessage(String searchText, String responseEndpoint, long uuid) {
        super(responseEndpoint, uuid);
        this.searchText = searchText;
    }

    public RequestSearchMessage(String searchText, Addressable responseAddress) {
        super(responseAddress);
        this.searchText = searchText;
    }

    public static StupidStreamObject getStupidStreamObject(String searchText, Addressable responseEndpoint) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_MESSAGES, responseEndpoint)
            .setProperty(KEY_SEARCH_TEXT, searchText);
    }

    public StupidStreamObject toStupidStreamObject() {
        return getStupidStreamObject(searchText, this);
    }

    @Override
    public String toString() {
        return "RequestSearchMessage{" +
            "searchText='" + searchText + '\'' +
            ", addres='" + super.toString() + '\'' +
            '}';
    }

    public String getSearchText() {
        return searchText;
    }
}
;