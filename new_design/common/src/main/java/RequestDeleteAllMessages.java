import java.util.logging.Logger;

public class RequestDeleteAllMessages extends BaseRequest {
    private static final Logger LOGGER = Logger.getLogger(RequestDeleteAllMessages.class.getName());

    public RequestDeleteAllMessages() {
        super(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES);
    }

    public RequestDeleteAllMessages(StupidStreamObject serializedRequest) {
        super(serializedRequest);

        if (serializedRequest.getObjectType() != StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES) {
            LOGGER.warning("StupidStreamObject doesn't have the correct object type");
            throw new RuntimeException("Incorrect object type");
        }
    }

    @Override
    StupidStreamObject toStupidStreamObject(Addressable responseAddress) {
        return new StupidStreamObject(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, responseAddress);
    }
}
