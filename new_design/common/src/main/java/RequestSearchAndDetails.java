import java.util.logging.Logger;

public class RequestSearchAndDetails extends BaseRequest {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchAndDetails.class.getName());
    private static final String KEY_SEARCH_TEXT = "searchText";

    private final String searchText;

    public RequestSearchAndDetails(String searchText) {
        super(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS);
        this.searchText = searchText;
    }

    public RequestSearchAndDetails(StupidStreamObject serializedRequest) {
        super(serializedRequest);

        if (serializedRequest.getObjectType() != StupidStreamObject.ObjectType.SEARCH_AND_DETAILS) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        this.searchText = serializedRequest.getProperty(KEY_SEARCH_TEXT);
    }

    @Override
    public StupidStreamObject toStupidStreamObject(Addressable responseAddress) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, responseAddress)
            .setProperty(KEY_SEARCH_TEXT, searchText);
    }

    public String getSearchText() {
        return searchText;
    }
}
