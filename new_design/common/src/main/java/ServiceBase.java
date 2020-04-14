public abstract class ServiceBase<Snap> {
    private String objectTypeToHandle;

    private Class<? extends BaseEvent> classOfObjectToHandle;
    protected int asyncHandleChannel;

    public ServiceBase(Class<? extends BaseEvent> classOfObjectToHandle, int asyncHandleChannel) {
        this.classOfObjectToHandle = classOfObjectToHandle;
        this.objectTypeToHandle = classOfObjectToHandle.getName();
        this.asyncHandleChannel = asyncHandleChannel;
    }

    public String getObjectTypeToHandle() {
        return objectTypeToHandle;
    }

    public Class<? extends BaseEvent> getClassOfObjectToHandle() {
        return classOfObjectToHandle;
    }

    abstract Response handleRequest(BaseEvent request,
                           JointStorageSystem<Snap> self,
                           Snap snapshot);

    @Override
    public String toString() {
        return "ServiceBase{" +
            "objectTypeToHandle='" + objectTypeToHandle + '\'' +
            ", asyncHandleChannel=" + asyncHandleChannel +
            '}';
    }
}
