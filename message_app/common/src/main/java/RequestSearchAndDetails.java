import java.util.logging.Logger;

public class RequestSearchAndDetails extends RequestSearchMessage {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchAndDetails.class.getName());

    public RequestSearchAndDetails(Addressable responseAddress, String searchText) {
        super(responseAddress, searchText);
    }
}
