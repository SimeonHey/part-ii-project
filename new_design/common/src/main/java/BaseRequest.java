public abstract class BaseRequest {
    private final StupidStreamObject.ObjectType objectType;

    public BaseRequest(StupidStreamObject serializedRequest) {
        this.objectType = serializedRequest.getObjectType();
    }

    public BaseRequest(StupidStreamObject.ObjectType objectType) {
        this.objectType = objectType;
    }

    public StupidStreamObject.ObjectType getObjectType() {
        return objectType;
    }

    abstract StupidStreamObject toStupidStreamObject(Addressable addressable);

    @Override
    public String toString() {
        return "BaseRequest{" +
            "objectType=" + objectType +
            '}';
    }
}
