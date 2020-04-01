import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class PsqlStorageSystemsFactory extends StorageSystemFactory<WrappedConnection> {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystemsFactory.class.getName());

    public PsqlStorageSystemsFactory(int psqlListenPort) throws IOException {
        super("psql", new PsqlSnapshottedWrapper(), psqlListenPort, (storageSystem) -> {
            var consumer = LoopingConsumer.fresh(
                storageSystem.fullName,
                Constants.TEST_KAFKA_ADDRESS,
                storageSystem.classMap);
            consumer.moveAllToLatest();
            consumer.subscribe(storageSystem::kafkaServiceHandler);
            Executors.newFixedThreadPool(1).submit(consumer::listenBlockingly);
        });
    }

    public PsqlStorageSystemsFactory(int psqlListenPort,
                                     Consumer<JointStorageSystem<WrappedConnection>> bootstrapProcedure) throws IOException {
        super("psql", new PsqlSnapshottedWrapper(), psqlListenPort, bootstrapProcedure);
    }

    @Override
    JointStorageSystem<WrappedConnection> simpleOlep() {
        return new JointStorageSystemBuilder<>("psql simple olep", httpStorageSystem, snapshottedWrapper,
            this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerHttpService(new ServiceBase<>(RequestAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerHttpService(new ServiceBase<>(RequestMessageDetails.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        (RequestMessageDetails) request);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            }).build();
    }

    @Override
    JointStorageSystem<WrappedConnection> serReads() {
        return new JointStorageSystemBuilder<>("psql ser reads", this.httpStorageSystem,
            this.snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(RequestMessageDetails.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        (RequestMessageDetails) request);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            }).build();
    }

    @Override
    JointStorageSystem<WrappedConnection> sdRequestNoSession() {
        return new JointStorageSystemBuilder<>("psql SD no session", this.httpStorageSystem,
            this.snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(RequestMessageDetails.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        (RequestMessageDetails) request);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(RequestSearchAndDetails.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails =
                        self.waitForContact(request.getResponseAddress().getChannelID(), RequestMessageDetails.class);

                    // Once it does, repeat what get message details does
                    var dbResponse = wrapper.getMessageDetails(snapshot, requestMessageDetails);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            }).build();
    }

    @Override
    JointStorageSystem<WrappedConnection> sdRequestSeparateSession() {
        return new JointStorageSystemBuilder<>("psql SD WITH session", this.httpStorageSystem,
            this.snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), 
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(RequestMessageDetails.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        (RequestMessageDetails) request);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(RequestSearchAndDetails.class, 0) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {

                    LOGGER.info(self.fullName + " waits to be contacted by Lucene...");
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails =
                        self.waitForContact(request.getResponseAddress().getChannelID(), RequestMessageDetails.class);
                    LOGGER.info("Contact successful! Request is: " + requestMessageDetails);

                    // Once it does, repeat what get message details does
                    var dbResponse = wrapper.getMessageDetails(snapshot, requestMessageDetails);

                    LOGGER.info(self.fullName + ": the response from the database is: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getEventType(),
                             request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            }).build();
    }

    @Override
    JointStorageSystem<WrappedConnection> concurReads() {
        return new JointStorageSystemBuilder<>("psql concur reads", this.httpStorageSystem,
            this.snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestAllMessages.class, 0) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    var response = new MultithreadedResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(), dbResponse);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse + "; the multithreaded response is: " + response);
                    responseCallback.accept(response);
                }
            })
            // GET MESSAGE DETAILS
            .registerKafkaService(new ServiceBase<>(RequestMessageDetails.class, 0) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {
                    var dbResponse = wrapper.getMessageDetails(snapshot,
                        (RequestMessageDetails) request);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(RequestSearchAndDetails.class, 0) {
                @Override
                void handleRequest(BaseEvent request,
                                   WrappedSnapshottedStorageSystem<WrappedConnection> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback,
                                   JointStorageSystem<WrappedConnection> self,
                                   WrappedConnection snapshot) {

                    LOGGER.info(self.fullName + " waits to be contacted by Lucene...");
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails =
                        self.waitForContact(request.getResponseAddress().getChannelID(), RequestMessageDetails.class);
                    LOGGER.info("Contact successful! Request is: " + requestMessageDetails);

                    // Once it does, repeat what get message details does
                    var dbResponse = wrapper.getMessageDetails(snapshot, requestMessageDetails);

                    LOGGER.info(self.fullName + ": the response from the database is: " + dbResponse);
                    responseCallback.accept(
                        new MultithreadedResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            }).build();
    }
}
