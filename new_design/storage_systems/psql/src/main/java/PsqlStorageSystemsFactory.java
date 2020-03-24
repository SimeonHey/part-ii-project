import java.io.IOException;
import java.sql.Connection;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class PsqlStorageSystemsFactory extends StorageSystemFactory<Connection> {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystemsFactory.class.getName());

    public PsqlStorageSystemsFactory(LoopingConsumer<Long, StupidStreamObject> loopingConsumer) throws IOException {
        super("psql", new PsqlSnapshottedWrapper(), Constants.PSQL_LISTEN_PORT, loopingConsumer);
    }

    public PsqlStorageSystemsFactory(LoopingConsumer<Long, StupidStreamObject> loopingConsumer,
                                     int psqlListenPort) throws IOException {
        super("psql", new PsqlSnapshottedWrapper(), psqlListenPort, loopingConsumer);
    }

    @Override
    JointStorageSystem<Connection> simpleOlep() {
        return new JointStorageSystem<>("psql simple olep", kafka, httpStorageSystem, snapshottedWrapper, -1)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.postMessage(new RequestPostMessage(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        new RequestAllMessages(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        new RequestMessageDetails(request));
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            });
    }

    @Override
    JointStorageSystem<Connection> serReads() {
        return new JointStorageSystem<>("psql ser reads", this.kafka, this.httpStorageSystem,
            this.snapshottedWrapper, -1)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.postMessage(new RequestPostMessage(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        new RequestAllMessages(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        new RequestMessageDetails(request));
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            });
    }

    @Override
    JointStorageSystem<Connection> sdRequestNoSession() {
        return new JointStorageSystem<>("psql SD no session", this.kafka, this.httpStorageSystem,
            this.snapshottedWrapper, -1)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.postMessage(new RequestPostMessage(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        new RequestAllMessages(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        new RequestMessageDetails(request));
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails = new RequestMessageDetails(
                        self.waitForContact(request.getResponseAddress().getChannelID(), StupidStreamObject.class));

                    // Once it does, repeat what get message details does
                    var dbResponse = wrapper.getMessageDetails(snapshot, requestMessageDetails);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            });
    }

    @Override
    JointStorageSystem<Connection> sdRequestSeparateSession() {
        return new JointStorageSystem<>("psql SD WITH session", this.kafka, this.httpStorageSystem,
            this.snapshottedWrapper, 1)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.postMessage(new RequestPostMessage(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        new RequestAllMessages(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        new RequestMessageDetails(request));
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, 0) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {

                    LOGGER.info(self.fullName + " waits to be contacted by Lucene...");
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails = new RequestMessageDetails(
                        self.waitForContact(request.getResponseAddress().getChannelID(), StupidStreamObject.class));
                    LOGGER.info("Contact successful! Request is: " + requestMessageDetails);

                    // Once it does, repeat what get message details does
                    var dbResponse = wrapper.getMessageDetails(snapshot, requestMessageDetails);

                    LOGGER.info(self.fullName + ": the response from the database is: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            });
    }

    @Override
    JointStorageSystem<Connection> concurReads() {
        return new JointStorageSystem<>("psql concur reads", this.kafka, this.httpStorageSystem,
            this.snapshottedWrapper, 1)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.postMessage(new RequestPostMessage(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, 0) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        new RequestAllMessages(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                        request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, 0) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        new RequestMessageDetails(request));
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, 0) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {

                    LOGGER.info(self.fullName + " waits to be contacted by Lucene...");
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails = new RequestMessageDetails(
                        self.waitForContact(request.getResponseAddress().getChannelID(), StupidStreamObject.class));
                    LOGGER.info("Contact successful! Request is: " + requestMessageDetails);

                    // Once it does, repeat what get message details does
                    var dbResponse = wrapper.getMessageDetails(snapshot, requestMessageDetails);

                    LOGGER.info(self.fullName + ": the response from the database is: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            });
    }

    @Override
    JointStorageSystem<Connection> concurSchedule() {
        return new JointStorageSystem<>("psql concur schedule", this.kafka, this.httpStorageSystem,
            this.snapshottedWrapper, 2)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.postMessage(new RequestPostMessage(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, -1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getObjectType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES - has its separate channel because it is slooooooow
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, 0) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        new RequestAllMessages(request));
                    var response = new MultithreadedResponse(self.shortName, request.getObjectType(),
                        request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, 1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        new RequestMessageDetails(request));
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, 1) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<Connection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<Connection> self,
                                   Connection snapshot) {

                    LOGGER.info(self.fullName + " waits to be contacted by Lucene...");
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails = new RequestMessageDetails(
                        self.waitForContact(request.getResponseAddress().getChannelID(), StupidStreamObject.class));
                    LOGGER.info("Contact successful! Request is: " + requestMessageDetails);

                    // Once it does, repeat what get message details does
                    var dbResponse = wrapper.getMessageDetails(snapshot, requestMessageDetails);

                    LOGGER.info(self.fullName + ": the response from the database is: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getObjectType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            });
    }
}
