import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class PsqlStorageSystemsFactory extends StorageSystemFactory<Connection> {
    private static final Logger LOGGER = Logger.getLogger(PsqlStorageSystemsFactory.class.getName());

    private final static PsqlSnapshottedSystem wrapper = new PsqlSnapshottedSystem();

    public PsqlStorageSystemsFactory(int psqlListenPort) throws IOException {
        super("psql", wrapper, psqlListenPort, (storageSystem) -> {
            var consumer = new LoopingConsumer(
                storageSystem.fullName,
                ConstantsMAPP.TEST_KAFKA_ADDRESS,
                ConstantsMAPP.KAFKA_TOPIC,
                storageSystem.classMap);
            consumer.moveAllToLatest();
            consumer.subscribe(storageSystem::kafkaActionHandler);
            Executors.newFixedThreadPool(1).submit(consumer::listenBlockingly);
        });

        wrapper.deleteAllMessages();
    }

    public PsqlStorageSystemsFactory(int psqlListenPort,
                                     Consumer<JointStorageSystem<Connection>> bootstrapProcedure) throws IOException {
        super("psql", new PsqlSnapshottedSystem(), psqlListenPort, bootstrapProcedure);

        wrapper.deleteAllMessages();
    }

    @Override
    JointStorageSystem<Connection> simpleOlep() {
        return new JointStorageSystemBuilder<>("psql simple olep", httpStorageSystem, snapshottedWrapper,
            this.bootstrapProcedure)
            // POST MESSAGE
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVO
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerAction(new ActionBase<>(RequestConvoMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    var dbResponse = wrapper.getConvoMessages(snapshot,
                        (RequestConvoMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerAction(new ActionBase<>(RequestMessageDetails.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            .registerAction(new ActionBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
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
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVO THREAD
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerAction(new ActionBase<>(RequestConvoMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    var dbResponse = wrapper.getConvoMessages(snapshot,
                        (RequestConvoMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerAction(new ActionBase<>(RequestMessageDetails.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            .registerAction(new ActionBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
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
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVO THREAD
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })

            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerAction(new ActionBase<>(RequestConvoMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    var dbResponse = wrapper.getConvoMessages(snapshot,
                        (RequestConvoMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerAction(new ActionBase<>(RequestMessageDetails.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            // SEARCH AND DETAILS
            .registerAction(new ActionBase<>(RequestSearchAndGetDetails.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    // This will block until Lucene contacts us
                    RequestMessageDetails requestMessageDetails =
                        self.waitForContact(request.getResponseAddress().getChannelID(), RequestMessageDetails.class);

                    // Once it does, repeat what get message details does
                    return new Response(wrapper.getMessageDetails(snapshot, requestMessageDetails));
                }
            })
            .registerAction(new ActionBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
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
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVO THREAD
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerAction(new ActionBase<>(RequestConvoMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    var dbResponse = wrapper.getConvoMessages(snapshot,
                        (RequestConvoMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerAction(new ActionBase<>(RequestMessageDetails.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            // SEARCH AND DETAILS
            .registerAction(new ActionBase<>(RequestSearchAndGetDetails.class, 0) {
                @Override
                Response handleEvent(EventBase request,
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
            .registerAction(new ActionBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
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
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVO THREAD
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // GET ALL MESSAGES
            .registerAction(new ActionBase<>(RequestConvoMessages.class, 0) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    var dbResponse = wrapper.getConvoMessages(snapshot,
                        (RequestConvoMessages) request);
                    LOGGER.info("Successfully executed the get all messages procedure in the wrapper; the database " +
                        "response is " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // GET MESSAGE DETAILS
            .registerAction(new ActionBase<>(RequestMessageDetails.class, 0) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<Connection> self,
                                     Connection snapshot) {
                    return new Response(wrapper.getMessageDetails(snapshot, (RequestMessageDetails) request));
                }
            })
            // SEARCH AND DETAILS
            .registerAction(new ActionBase<>(RequestSearchAndGetDetails.class, 0) {
                @Override
                Response handleEvent(EventBase request,
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
            .registerAction(new ActionBase<>(RequestSleep1.class, 1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<Connection> self, Connection snapshot) {
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
