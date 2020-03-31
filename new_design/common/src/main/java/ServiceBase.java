import java.util.function.Consumer;

public abstract class ServiceBase<Snap extends AutoCloseable> {
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

    abstract void handleRequest(BaseEvent request,
                                WrappedSnapshottedStorageSystem<Snap> wrapper,
                                Consumer<MultithreadedResponse> responseCallback,
                                JointStorageSystem<Snap> self,
                                Snap snapshot);
}
