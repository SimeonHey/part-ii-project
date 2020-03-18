import java.util.logging.Logger;

public class RequestSearchAndDetails extends Addressable {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchAndDetails.class.getName());
    private static final String KEY_SEARCH_TEXT = "searchText";

    private final String searchText;

    RequestSearchAndDetails(String searchText, String responseEndpoint, long requestUUID) {
        super(responseEndpoint, requestUUID);
        this.searchText = searchText;
    }

    public RequestSearchAndDetails(String searchText, Addressable addressable) {
        super(addressable);
        this.searchText = searchText;
    }

    static RequestSearchAndDetails fromStupidStreamObject(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.SEARCH_AND_DETAILS) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        String searchText = stupidStreamObject.getProperty(KEY_SEARCH_TEXT);
        return new RequestSearchAndDetails(searchText, stupidStreamObject.getResponseAddress());
    }

    public static StupidStreamObject getStupidStreamObject(String searchText, Addressable addressable) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, addressable)
            .setProperty(KEY_SEARCH_TEXT, searchText);
    }

    public StupidStreamObject toStupidStreamObject() {
        return getStupidStreamObject(searchText, this);
    }

    public String getSearchText() {
        return searchText;
    }
}
