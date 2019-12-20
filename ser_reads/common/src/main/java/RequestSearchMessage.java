import java.util.logging.Logger;

public class RequestSearchMessage {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchMessage.class.getName());
    private static final String KEY_SEARCH_TEXT = "searchText";
    private static final String KEY_RESPONSE_ENDPOINT = "responseEndpoint";

    private final String searchText;
    private final String responseEndpoint;

    public RequestSearchMessage(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.SEARCH_MESSAGES) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        this.searchText = stupidStreamObject.getProperty(KEY_SEARCH_TEXT);
        this.responseEndpoint = stupidStreamObject.getProperty(KEY_RESPONSE_ENDPOINT);
    }

    public RequestSearchMessage(String searchText, String responseEndpoint) {
        this.searchText = searchText;
        this.responseEndpoint = responseEndpoint;
    }

    public StupidStreamObject toStupidStreamObject() {
        return new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_MESSAGES)
            .setProperty(KEY_SEARCH_TEXT, this.searchText)
            .setProperty(KEY_RESPONSE_ENDPOINT, this.responseEndpoint);
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

    public String getResponseEndpoint() {
        return responseEndpoint;
    }
}
;