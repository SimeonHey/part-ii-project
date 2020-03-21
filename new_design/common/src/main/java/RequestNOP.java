import java.util.logging.Logger;

public class RequestNOP extends BaseRequest {
    private static final Logger LOGGER = Logger.getLogger(RequestNOP.class.getName());

    public RequestNOP(StupidStreamObject serializedRequest) {
        super(serializedRequest);
        if (serializedRequest.getObjectType() != StupidStreamObject.ObjectType.NOP) {
            LOGGER.warning("Error in RequestNOP");
            throw new RuntimeException("Incorrect object type");
        }
    }

    public RequestNOP() {
        super(StupidStreamObject.ObjectType.NOP);
    }

    public StupidStreamObject toStupidStreamObject(Addressable responseAddress) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.NOP, responseAddress);
    }
}
