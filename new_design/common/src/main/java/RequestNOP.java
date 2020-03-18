import java.util.logging.Logger;

public class RequestNOP extends Addressable {
    private static final Logger LOGGER = Logger.getLogger(RequestNOP.class.getName());

    public RequestNOP(StupidStreamObject stupidStreamObject) {
        super(stupidStreamObject.getResponseAddress());
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.NOP) {
            LOGGER.warning("Error in RequestNOP");
            throw new RuntimeException("Incorrect object type");
        }
    }

    public static StupidStreamObject toStupidStreamObject(Addressable responseAddress) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.NOP, responseAddress);
    }
}
