import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class PsqlStorageSystemsFactory extends StorageSystemFactory<Connection> {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystemsFactory.class.getName());

    public PsqlStorageSystemsFactory(ExecutorService executorService) throws IOException {
        super("psql", executorService, new PsqlSnapshottedWrapper(), Constants.PSQL_LISTEN_PORT);
    }

    @Override
    public JointStorageSystem<Connection> simpleOlep() {
        return new JointStorageSystem<>("PSQL simple olep", loopingKafka, httpStorageSystem, snapshottedWrapper)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), null);
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), null);
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    var dbResponse = wrapper.getAllMessages(wrapper.getDefaultSnapshot(),
                        RequestAllMessages.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    var dbResponse = wrapper.getMessageDetails(wrapper.getDefaultSnapshot(),
                        RequestMessageDetails.fromStupidStreamObject(request));
                    responseCallback.accept(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH MESSAGE
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    LOGGER.info("PSQL simple olep skips search request");
                }
            })
            // NOP
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.NOP, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    LOGGER.info("PSQL simple olep received NOP");
//                    responseCallback.accept(new StupidStreamObject(StupidStreamObject.ObjectType.NOP,
//                        Constants.NO_RESPONSE));
                }
            });
    }

    @Override
    JointStorageSystem<Connection> serReads() {
        return new JointStorageSystem<>("PSQL ser reads", this.loopingKafka, this.httpStorageSystem,
            this.snapshottedWrapper)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), null);
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), null);
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    var dbResponse = wrapper.getAllMessages(wrapper.getDefaultSnapshot(),
                        RequestAllMessages.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    var dbResponse = wrapper.getMessageDetails(wrapper.getDefaultSnapshot(),
                        RequestMessageDetails.fromStupidStreamObject(request));
                    responseCallback.accept(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    LOGGER.info("PSQL simple olep skips search request");
                }
            })
            // NOP
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.NOP, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    LOGGER.info("PSQL simple olep received NOP");
//                    responseCallback.accept(new StupidStreamObject(StupidStreamObject.ObjectType.NOP,
//                        Constants.NO_RESPONSE));
                }
            });
    }
}
