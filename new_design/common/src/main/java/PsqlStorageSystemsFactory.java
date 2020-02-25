import java.io.IOException;
import java.sql.Connection;
import java.util.function.Supplier;

public class PsqlStorageSystemsFactory {

    public static JointStorageSystem<Connection> simpleOlep() throws IOException {
        LoopingConsumer<Long, StupidStreamObject> aLoopingKafka =
            new LoopingConsumer<>(KafkaUtils.createConsumer("psql",
                Constants.KAFKA_ADDRESS, Constants.KAFKA_TOPIC));

        HttpStorageSystem aHttpStorageSystem = new HttpStorageSystem("psql",
            HttpUtils.initHttpServer(Integer.parseInt(Constants.PSQL_LISTEN_PORT)));

        Connection connectionToUse = SqlUtils.obtainConnection(Constants.PSQL_USER_PASS[0],
            Constants.PSQL_USER_PASS[1], Constants.PSQL_ADDRESS);

        Supplier<Connection> connectionSupplier = () -> connectionToUse;
        PsqlSnapshottedWrapper snapshottedWrapper = new PsqlSnapshottedWrapper(connectionSupplier);
        return new JointStorageSystem<>(aLoopingKafka, aHttpStorageSystem, snapshottedWrapper)
            .registerService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                StupidStreamObject handleRequest(StupidStreamObject request,
                                                 WrappedSnapshottedStorageSystem<Connection> wrapper) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request, 0L));
                    return new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE);
                }
            })
            .registerService(new ServiceBase<Connection>() {
                @Override
                StupidStreamObject handleRequest(StupidStreamObject request, WrappedSnapshottedStorageSystem<Connection> wrapper) {
                    return null;
                }
            }); // TODO: Write handlers for all requests
    }
}
