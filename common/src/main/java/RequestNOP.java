import java.util.logging.Logger;

public class RequestNOP {
    private static final Logger LOGGER = Logger.getLogger(RequestNOP.class.getName());

    public RequestNOP(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.NOP) {
            LOGGER.warning("Error in RequestNOP");
            throw new RuntimeException("Incorrect object type");
        }
    }

    public static StupidStreamObject toStupidStreamObject() {
        return new StupidStreamObject(StupidStreamObject.ObjectType.NOP);
    }
}
