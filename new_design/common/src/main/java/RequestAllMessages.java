import java.util.logging.Logger;

public class RequestAllMessages extends BaseRequest {
    private static final Logger LOGGER = Logger.getLogger(RequestAllMessages.class.getName());

    RequestAllMessages() {
        super(StupidStreamObject.ObjectType.GET_ALL_MESSAGES);
    }

    RequestAllMessages(StupidStreamObject serializedRequest) {
        super(serializedRequest);
        if (serializedRequest.getObjectType() != StupidStreamObject.ObjectType.GET_ALL_MESSAGES) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }
    }

    @Override
    public StupidStreamObject toStupidStreamObject(Addressable responseAddress) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, responseAddress);
    }
}
