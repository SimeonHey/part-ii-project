public class ConfirmationResponse {
    private final String fromStorageSystem;
    private final String objectType;

    public ConfirmationResponse(String fromStorageSystem, String objectType) {
        this.fromStorageSystem = fromStorageSystem;
        this.objectType = objectType;
    }

    public String getFromStorageSystem() {
        return fromStorageSystem;
    }

    public String getObjectType() {
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
