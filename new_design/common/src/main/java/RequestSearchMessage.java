import java.util.logging.Logger;

public class RequestSearchMessage extends BaseRequest {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchMessage.class.getName());
    private static final String KEY_SEARCH_TEXT = "searchText";

    private final String searchText;

    public RequestSearchMessage(String searchText) {
        super(StupidStreamObject.ObjectType.SEARCH_MESSAGES);
        this.searchText = searchText;
    }

    public RequestSearchMessage(StupidStreamObject serializedRequest) {
        super(serializedRequest);

        // Either one of Search or S&D is fine
        if (serializedRequest.getObjectType() != StupidStreamObject.ObjectType.SEARCH_MESSAGES &&
            serializedRequest.getObjectType() != StupidStreamObject.ObjectType.SEARCH_AND_DETAILS) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        this.searchText = serializedRequest.getProperty(KEY_SEARCH_TEXT);
    }

    public static StupidStreamObject getStupidStreamObject(String searchText, Addressable responseEndpoint) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_MESSAGES, responseEndpoint)
            .setProperty(KEY_SEARCH_TEXT, searchText);
    }

    @Override
    public StupidStreamObject toStupidStreamObject(Addressable responseAddress) {
        return getStupidStreamObject(searchText, responseAddress);
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
