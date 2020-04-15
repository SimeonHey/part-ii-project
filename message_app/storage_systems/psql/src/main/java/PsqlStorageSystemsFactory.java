import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class PsqlStorageSystemsFactory extends StorageSystemFactory<Connection> {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystemsFactory.class.getName());

    private final static PsqlSnapshottedWrapper wrapper = new PsqlSnapshottedWrapper();

    public PsqlStorageSystemsFactory(int psqlListenPort) throws IOException {
        super("psql", wrapper, psqlListenPort, (storageSystem) -> {
            var consumer = new LoopingConsumer(
                storageSystem.fullName,
                ConstantsMAPP.TEST_KAFKA_ADDRESS,
                ConstantsMAPP.KAFKA_TOPIC,
                storageSystem.classMap);
            consumer.moveAllToLatest();
            consumer.subscribe(storageSystem::kafkaServiceHandler);
            Executors.newFixedThreadPool(1).submit(consumer::listenBlockingly);
        });

        wrapper.deleteAllMessages();
    }

    public PsqlStorageSystemsFactory(int psqlListenPort,
                                     Consumer<JointStorageSystem<Connection>> bootstrapProcedure) throws IOException {
        super("psql", new PsqlSnapshottedWrapper(), psqlListenPort, bootstrapProcedure);

        wrapper.deleteAllMessages();
    }

    @Override
    JointStorageSystem<Connection> simpleOlep() {
        return new JointStorageSystemBuilder<>("psql simple olep", httpStorageSystem, snapshottedWrapper,
            this.bootstrapProcedure)
            // POST MESSAGE
            .registerService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerService(new ServiceBase<>(RequestAllMessages.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerService(new ServiceBase<>(RequestMessageDetails.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            .registerService(new ServiceBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleRequest(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }

                    return Response.CONFIRMATION;
                }
            }).build();
    }

    @Override
    JointStorageSystem<Connection> serReads() {
        return new JointStorageSystemBuilder<>("psql ser reads", this.httpStorageSystem,
            this.snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerService(new ServiceBase<>(RequestAllMessages.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerService(new ServiceBase<>(RequestMessageDetails.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            .registerService(new ServiceBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleRequest(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }

                    return Response.CONFIRMATION;
                }
            }).build();
    }

    @Override
    JointStorageSystem<Connection> sdRequestNoSession() {
        return new JointStorageSystemBuilder<>("psql SD no session", this.httpStorageSystem,
            this.snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerService(new ServiceBase<>(RequestAllMessages.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerService(new ServiceBase<>(RequestMessageDetails.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            // SEARCH AND DETAILS
            .registerService(new ServiceBase<>(RequestSearchAndDetails.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails =
                        self.waitForContact(request.getResponseAddress().getChannelID(), RequestMessageDetails.class);

                    // Once it does, repeat what get message details does
                    return new Response(wrapper.getMessageDetails(snapshot, requestMessageDetails));
                }
            })
            .registerService(new ServiceBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleRequest(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }

                    return Response.CONFIRMATION;
                }
            }).build();
    }

    @Override
    JointStorageSystem<Connection> sdRequestSeparateSession() {
        return new JointStorageSystemBuilder<>("psql SD WITH session", this.httpStorageSystem,
            this.snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerService(new ServiceBase<>(RequestAllMessages.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerService(new ServiceBase<>(RequestMessageDetails.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            // SEARCH AND DETAILS
            .registerService(new ServiceBase<>(RequestSearchAndDetails.class, 0) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {

                    LOGGER.info(self.fullName + " waits to be contacted by Lucene...");
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails =
                        self.waitForContact(request.getResponseAddress().getChannelID(), RequestMessageDetails.class);
                    LOGGER.info("Contact successful! Request is: " + requestMessageDetails);

                    // Once it does, repeat what get message details does
                    var dbResponse = wrapper.getMessageDetails(snapshot, requestMessageDetails);

                    LOGGER.info(self.fullName + ": the response from the database is: " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            .registerService(new ServiceBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleRequest(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }

                    return Response.CONFIRMATION;
                }
            }).build();
    }

    @Override
    JointStorageSystem<Connection> concurReads() {
        return new JointStorageSystemBuilder<>("psql concur reads", this.httpStorageSystem,
            this.snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerService(new ServiceBase<>(RequestAllMessages.class, 0) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    var dbResponse = wrapper.getAllMessages(snapshot,
                        (RequestAllMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerService(new ServiceBase<>(RequestMessageDetails.class, 0) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            // SEARCH AND DETAILS
            .registerService(new ServiceBase<>(RequestSearchAndDetails.class, 0) {
                @Override
                Response handleRequest(EventBase request,
                                       JointStorageSystem<Connection> self,
                                       Connection snapshot) {

                    LOGGER.info(self.fullName + " waits to be contacted by Lucene...");
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails =
                        self.waitForContact(request.getResponseAddress().getChannelID(), RequestMessageDetails.class);
                    LOGGER.info("Contact successful! Request is: " + requestMessageDetails);

                    // Once it does, repeat what get message details does
                    var dbResponse = wrapper.getMessageDetails(snapshot, requestMessageDetails);

                    LOGGER.info(self.fullName + ": the response from the database is: " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            .registerService(new ServiceBase<>(RequestSleep1.class, 1) {
                @Override
                Response handleRequest(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }

                    return Response.CONFIRMATION;
                }
            }).build();
    }
}
