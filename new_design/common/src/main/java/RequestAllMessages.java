import java.util.logging.Logger;

public class RequestAllMessages extends Addressable {
    private static final Logger LOGGER = Logger.getLogger(RequestAllMessages.class.getName());

    RequestAllMessages(Addressable addressable) {
        super(addressable);
    }

    static RequestAllMessages fromStupidStreamObject(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.GET_ALL_MESSAGES) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }

        return new RequestAllMessages(stupidStreamObject.getResponseAddress());
    }

    public StupidStreamObject toStupidStreamObject() {
        return new StupidStreamObject(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, this);
    }
}
