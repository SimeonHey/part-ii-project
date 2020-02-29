import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class PsqlStorageSystemsFactory {
    public static JointStorageSystem<Connection> simpleOlep(ExecutorService executorService) throws IOException {
        LoopingConsumer<Long, StupidStreamObject> aLoopingKafka =
            new LoopingConsumer<>(KafkaUtils.createConsumer("psql",
                Constants.KAFKA_ADDRESS, Constants.KAFKA_TOPIC));

        HttpStorageSystem aHttpStorageSystem = new HttpStorageSystem("psql",
            HttpUtils.initHttpServer(Integer.parseInt(Constants.PSQL_LISTEN_PORT)));

        PsqlSnapshottedWrapper snapshottedWrapper = new PsqlSnapshottedWrapper();
        var ss = new JointStorageSystem<>(
            "PSQL simple olep",
            aLoopingKafka,
            aHttpStorageSystem,
            snapshottedWrapper)

            // POST MESSAGE
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
            // DELETE ALL MESSAGES
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
            .registerService(new ServiceBase<Connection>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request, WrappedSnapshottedStorageSystem<Connection> wrapper, Consumer<StupidStreamObject> responseCallback) {
                    var response = wrapper.getAllMessages(wrapper.getDefaultSnapshot(),
                        RequestAllMessages.fromStupidStreamObject(request, 0L)).toStupidStreamObject();
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
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
            // SEARCH MESSAGE
            .registerService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<StupidStreamObject> responseCallback) {
                    responseCallback.accept(
                        new StupidStreamObject(StupidStreamObject.ObjectType.SEARCH_MESSAGES, Constants.NO_RESPONSE));
                }
            })
            // NOP
            .registerService(new ServiceBase<Connection>(StupidStreamObject.ObjectType.NOP, false) {
                @Override
                void handleRequest(StupidStreamObject request, WrappedSnapshottedStorageSystem<Connection> wrapper, Consumer<StupidStreamObject> responseCallback) {
//                    responseCallback.accept(new StupidStreamObject(StupidStreamObject.ObjectType.NOP,
//                        Constants.NO_RESPONSE));
                }
            })
            ;

        executorService.submit(aLoopingKafka::listenBlockingly);
        return ss;
    }
}
