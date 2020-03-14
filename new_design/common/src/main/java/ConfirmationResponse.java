public class ConfirmationResponse {
    private final String fromStorageSystem;
    private final StupidStreamObject.ObjectType objectType;

    public ConfirmationResponse(String fromStorageSystem, StupidStreamObject.ObjectType objectType) {
        this.fromStorageSystem = fromStorageSystem;
        this.objectType = objectType;
    }

    public String getFromStorageSystem() {
        return fromStorageSystem;
    }

    public StupidStreamObject.ObjectType getObjectType() {
        return objectType;
    }

    @Override
    public String toString() {
        return "ConfirmationResponse{" +
            "fromStorageSystem='" + fromStorageSystem + '\'' +
            ", objectType=" + objectType +
            '}';
    }
}
