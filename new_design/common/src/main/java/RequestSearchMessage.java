import java.util.logging.Logger;

public class RequestSearchMessage extends BaseEvent {
    private static final Logger LOGGER = Logger.getLogger(RequestSearchMessage.class.getName());
    private final String searchText;

    public RequestSearchMessage(Addressable responseAddress, String searchText) {
        super(responseAddress, true);
        this.searchText = searchText;
    }

    @Override
    public String toString() {
        return "RequestSearchMessage{" +
            "searchText length = '" + searchText.length() + '\'' +
            ", addres='" + super.toString() + '\'' +
            '}';
    }

    public String getSearchText() {
        return searchText;
    }
}
