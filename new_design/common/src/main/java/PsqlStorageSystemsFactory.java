import java.io.IOException;
import java.sql.Connection;
import java.util.function.Consumer;

public class PsqlStorageSystemsFactory {
    public static JointStorageSystem<Connection> simpleOlep() throws IOException {
        LoopingConsumer<Long, StupidStreamObject> aLoopingKafka =
            new LoopingConsumer<>(KafkaUtils.createConsumer("psql",
                Constants.KAFKA_ADDRESS, Constants.KAFKA_TOPIC));

        HttpStorageSystem aHttpStorageSystem = new HttpStorageSystem("psql",
            HttpUtils.initHttpServer(Integer.parseInt(Constants.PSQL_LISTEN_PORT)));

        PsqlSnapshottedWrapper snapshottedWrapper = new PsqlSnapshottedWrapper();
        return new JointStorageSystem<>(aLoopingKafka, aHttpStorageSystem, snapshottedWrapper)
            .registerService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                                 WrappedSnapshottedStorageSystem<Connection> wrapper,
                                                 Consumer<StupidStreamObject> responseCallback) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request, 0L)); // TODO: ID
                    var response = new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE,
                        Constants.NO_RESPONSE);
                    responseCallback.accept(response);
                }
            })
            .registerService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<StupidStreamObject> responseCallback) {
                    wrapper.deleteAllMessages();
                    var response = new StupidStreamObject(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES,
                        Constants.NO_RESPONSE);
                    responseCallback.accept(response);
                }
            })
            .registerService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<StupidStreamObject> responseCallback) {
                    var response = wrapper.getMessageDetails(wrapper.getDefaultSnapshot(),
                        RequestMessageDetails.fromStupidStreamObject(request, 0L)).toStupidStreamObject(); // TODO: ID
                    responseCallback.accept(response);
                }
            })
            .registerService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<StupidStreamObject> responseCallback) {
                    responseCallback.accept(
                        new StupidStreamObject(StupidStreamObject.ObjectType.NOP, Constants.NO_RESPONSE));
                }
            });
    }
}
