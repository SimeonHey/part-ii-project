import java.util.logging.Logger;

public class RequestSearchAndDetails extends RequestWithResponse {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchAndDetails.class.getName());
    private static final String KEY_SEARCH_TEXT = "searchText";

    private final String searchText;

    RequestSearchAndDetails(String searchText, String responseEndpoint, long requestUUID) {
        super(responseEndpoint, requestUUID);
        this.searchText = searchText;
    }

    static RequestSearchAndDetails fromStupidStreamObject(StupidStreamObject stupidStreamObject, long requestUUID) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.SEARCH_AND_DETAILS) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        String searchText = stupidStreamObject.getProperty(KEY_SEARCH_TEXT);
        String responseEndpoint = stupidStreamObject.getProperty(KEY_RESPONSE_ENDPOINT);

        return new RequestSearchAndDetails(searchText, responseEndpoint, requestUUID);
    }

    public static StupidStreamObject getStupidStreamObject(String searchText, String responseEndpoint) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, responseEndpoint)
            .setProperty(KEY_SEARCH_TEXT, searchText)
            .setProperty(KEY_RESPONSE_ENDPOINT, responseEndpoint);
    }

    public String getSearchText() {
        return searchText;
    }
}
