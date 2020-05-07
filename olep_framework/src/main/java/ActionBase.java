public abstract class ActionBase<Snap> {
    private String eventTypeToHandle;

    private Class<? extends EventBase> classOfObjectToHandle;
    protected boolean handleConcurrentlyWithSnapshot;

    public ActionBase(Class<? extends EventBase> classOfObjectToHandle, boolean handleConcurrentlyWithSnapshot) {
        this.classOfObjectToHandle = classOfObjectToHandle;
        this.eventTypeToHandle = classOfObjectToHandle.getName();
        this.handleConcurrentlyWithSnapshot = handleConcurrentlyWithSnapshot;
    }

    public String getEventTypeToHandle() {
        return eventTypeToHandle;
    }

    public Class<? extends EventBase> getClassOfObjectToHandle() {
        return classOfObjectToHandle;
    }

    abstract Response handleEvent(EventBase request,
                                  StorageSystem<Snap> self,
                                  Snap snapshot);

    @Override
    public String toString() {
        return "ServiceBase{" +
            "objectTypeToHandle='" + eventTypeToHandle + '\'' +
            ", asyncHandleChannel=" + handleConcurrentlyWithSnapshot +
            '}';
    }
}
