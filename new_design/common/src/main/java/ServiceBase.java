public abstract class ServiceBase<Snap extends AutoCloseable> {
    private StupidStreamObject.ObjectType objectTypeToHandle;
    protected boolean handleAsyncWithSnapshot;

    public ServiceBase(StupidStreamObject.ObjectType objectTypeToHandle, boolean handleAsyncWithSnapshot) {
        this.objectTypeToHandle = objectTypeToHandle;
        this.handleAsyncWithSnapshot = handleAsyncWithSnapshot;
    }

    boolean couldHandle(StupidStreamObject sso) {
        return sso.getObjectType().equals(this.objectTypeToHandle);
    }

    abstract StupidStreamObject handleRequest(StupidStreamObject request,
                                              WrappedSnapshottedStorageSystem<Snap> wrapper);
}
