import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class LuceneStorageSystemFactory extends StorageSystemFactory<IndexReader> {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystemFactory.class.getName());

    public LuceneStorageSystemFactory(LoopingConsumer<Long, StupidStreamObject> loopingConsumer) throws IOException {
        super("lucene", new LuceneSnapshottedWrapper(), Constants.LUCENE_LISTEN_PORT, loopingConsumer);
    }

    @Override
    public JointStorageSystem<IndexReader> simpleOlep() {
        return new JointStorageSystem<>("lucene simple olep", kafka, httpStorageSystem,
            snapshottedWrapper)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get all messages request and does nothing");
                }
            })
            // GET MESSAGE DETAILS
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get message details request and does nothing");
                }
            })
            // SEARCH MESSAGE
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(wrapper.getDefaultSnapshot(),
                        RequestSearchMessage.fromStupidStreamObject(request));
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // NOP
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.NOP, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info(self.name + " received NOP");
                }
            });
    }

    @Override
    JointStorageSystem<IndexReader> serReads() {
        return new JointStorageSystem<>("lucene ser reads", kafka, httpStorageSystem,
            snapshottedWrapper)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get all messages request and does nothing");
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get message details request and does nothing");
                }
            })
            // SEARCH MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(wrapper.getDefaultSnapshot(),
                        RequestSearchMessage.fromStupidStreamObject(request));
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // NOP
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.NOP, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info(self.name + " received NOP");
                }
            });
    }

    @Override
    JointStorageSystem<IndexReader> sdRequestNoSession() {
        return new JointStorageSystem<>("lucene sd no session", kafka, httpStorageSystem,
            snapshottedWrapper)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get all messages request and does nothing");
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get message details request and does nothing");
                }
            })
            // SEARCH MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(wrapper.getDefaultSnapshot(),
                        RequestSearchMessage.fromStupidStreamObject(request));
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    RequestSearchMessage requestSearchMessage = RequestSearchMessage.fromStupidStreamObject(request);
                    ResponseSearchMessage responseSearchMessage = wrapper.searchMessage(wrapper.getDefaultSnapshot(),
                        requestSearchMessage);

                    long idToLookFor = responseSearchMessage.getOccurrences().size() == 0
                        ? -1
                        : responseSearchMessage.getOccurrences().get(0);
                    var nextRequest = new RequestMessageDetails(idToLookFor, request.getResponseAddress());
                    String serialized = Constants.gson.toJson(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), nextRequest));

                    try {
                        HttpUtils.httpRequestResponse(Constants.LUCENE_PSQL_CONTACT_ENDPOINT, serialized);
                    } catch (IOException e) {
                        LOGGER.warning("Error when trying to contact psql for next hop of the request");
                        throw new RuntimeException(e);
                    }
                }
            })
            // NOP
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.NOP, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info(self.name + " received NOP");
                }
            });
    }

    @Override
    JointStorageSystem<IndexReader> sdRequestSeparateSession() {
        return new JointStorageSystem<>("lucene SD WITH session", kafka, httpStorageSystem,
            snapshottedWrapper)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get all messages request and does nothing");
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get message details request and does nothing");
                }
            })
            // SEARCH MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(wrapper.getDefaultSnapshot(),
                        RequestSearchMessage.fromStupidStreamObject(request));
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, true) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    RequestSearchMessage requestSearchMessage = RequestSearchMessage.fromStupidStreamObject(request);
                    ResponseSearchMessage responseSearchMessage = wrapper.searchMessage(snapshot, requestSearchMessage);

                    long idToLookFor = responseSearchMessage.getOccurrences().size() == 0
                        ? -1
                        : responseSearchMessage.getOccurrences().get(0);
                    var nextRequest = new RequestMessageDetails(idToLookFor, request.getResponseAddress());
                    LOGGER.info("Contacting PSQL with details request: " + nextRequest);

                    String serialized = Constants.gson.toJson(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), nextRequest));
                    try {
                        HttpUtils.httpRequestResponse(Constants.LUCENE_PSQL_CONTACT_ENDPOINT, serialized);
                    } catch (IOException e) {
                        LOGGER.warning("Error when trying to contact psql for next hop of the request");
                        throw new RuntimeException(e);
                    }
                }
            })
            // NOP
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.NOP, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info(self.name + " received NOP");
                }
            });
    }

    @Override
    JointStorageSystem<IndexReader> concurReads() {
        return new JointStorageSystem<>("lucene SD WITH session", kafka, httpStorageSystem,
            snapshottedWrapper)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.name, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, true) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get all messages request and does nothing");
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, true) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info("Lucene received a get message details request and does nothing");
                }
            })
            // SEARCH MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, true) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(wrapper.getDefaultSnapshot(),
                        RequestSearchMessage.fromStupidStreamObject(request));
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, true) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    RequestSearchMessage requestSearchMessage = RequestSearchMessage.fromStupidStreamObject(request);
                    ResponseSearchMessage responseSearchMessage = wrapper.searchMessage(snapshot, requestSearchMessage);

                    long idToLookFor = responseSearchMessage.getOccurrences().size() == 0
                        ? -1
                        : responseSearchMessage.getOccurrences().get(0);
                    var nextRequest = new RequestMessageDetails(idToLookFor, request.getResponseAddress());
                    LOGGER.info("Contacting PSQL with details request: " + nextRequest);

                    String serialized = Constants.gson.toJson(
                        new MultithreadedResponse(request.getResponseAddress().getChannelID(), nextRequest));
                    try {
                        HttpUtils.httpRequestResponse(Constants.LUCENE_PSQL_CONTACT_ENDPOINT, serialized);
                    } catch (IOException e) {
                        LOGGER.warning("Error when trying to contact psql for next hop of the request");
                        throw new RuntimeException(e);
                    }
                }
            })
            // NOP
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.NOP, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    LOGGER.info(self.name + " received NOP");
                }
            });
    }
}
