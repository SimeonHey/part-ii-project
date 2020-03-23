import java.util.function.Consumer;

public abstract class ServiceBase<Snap extends AutoCloseable> {
    private StupidStreamObject.ObjectType objectTypeToHandle;
    protected int asyncHandleChannel;

    public ServiceBase(StupidStreamObject.ObjectType objectTypeToHandle, int asyncHandleChannel) {
        this.objectTypeToHandle = objectTypeToHandle;
        this.asyncHandleChannel = asyncHandleChannel;
    }

    public StupidStreamObject.ObjectType getObjectTypeToHandle() {
        return objectTypeToHandle;
    }

    abstract void handleRequest(StupidStreamObject request,
                                WrappedSnapshottedStorageSystem<Snap> wrapper,
                                Consumer<MultithreadedResponse> responseCallback,
                                JointStorageSystem<Snap> self,
                                Snap snapshot);
}
