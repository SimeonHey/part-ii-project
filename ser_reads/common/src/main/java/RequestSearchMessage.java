import java.util.logging.Logger;

public class RequestSearchMessage extends RequestWithResponse {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchMessage.class.getName());
    private static final String KEY_SEARCH_TEXT = "searchText";

    private final String searchText;

    public static RequestSearchMessage fromStupidStreamObject(StupidStreamObject stupidStreamObject, long uuid) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.SEARCH_MESSAGES) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        String searchText = stupidStreamObject.getProperty(KEY_SEARCH_TEXT);
        String responseEndpoint = stupidStreamObject.getProperty(KEY_RESPONSE_ENDPOINT);

        return new RequestSearchMessage(searchText, responseEndpoint, uuid);
    }

    public RequestSearchMessage(String searchText, String responseEndpoint, long uuid) {
        super(responseEndpoint, uuid);
        this.searchText = searchText;
    }

    public static StupidStreamObject getStupidStreamObject(String searchText, String responseEndpoint) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_MESSAGES)
            .setProperty(KEY_SEARCH_TEXT, searchText)
            .setProperty(KEY_RESPONSE_ENDPOINT, responseEndpoint);
    }

    @Override
    public String toString() {
        return "RequestSearchMessage{" +
            "searchText='" + searchText + '\'' +
            ", responseEndpoint='" + responseEndpoint + '\'' +
            '}';
    }

    public String getSearchText() {
        return searchText;
    }
}
;