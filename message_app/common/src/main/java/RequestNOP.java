import java.util.logging.Logger;

public class RequestNOP extends EventBase {
    private static final Logger LOGGER = Logger.getLogger(RequestNOP.class.getName());

    public RequestNOP(Addressable responseAddress) {
        super(responseAddress, false);
    }
}
