public abstract class ServiceBase<Snap> {
    private String eventTypeToHandle;

    private Class<? extends EventBase> classOfObjectToHandle;
    protected int asyncHandleChannel;

    public ServiceBase(Class<? extends EventBase> classOfObjectToHandle, int asyncHandleChannel) {
        this.classOfObjectToHandle = classOfObjectToHandle;
        this.eventTypeToHandle = classOfObjectToHandle.getName();
        this.asyncHandleChannel = asyncHandleChannel;
    }

    public String getEventTypeToHandle() {
        return eventTypeToHandle;
    }

    public Class<? extends EventBase> getClassOfObjectToHandle() {
        return classOfObjectToHandle;
    }

    abstract Response handleEvent(EventBase request,
                                  JointStorageSystem<Snap> self,
                                  Snap snapshot);

    @Override
    public String toString() {
        return "ServiceBase{" +
            "objectTypeToHandle='" + eventTypeToHandle + '\'' +
            ", asyncHandleChannel=" + asyncHandleChannel +
            '}';
    }
}
