import java.util.logging.Logger;

public class RequestSearchAndGetDetails extends RequestSearchMessage {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchAndGetDetails.class.getName());

    public RequestSearchAndGetDetails(Addressable responseAddress, String searchText) {
        super(responseAddress, searchText);
    }
}
