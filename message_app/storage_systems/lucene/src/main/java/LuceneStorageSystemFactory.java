import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class LuceneStorageSystemFactory extends StorageSystemFactory<IndexReader> {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystemFactory.class.getName());

    private static final LuceneSnapshottedSystem wrapper = new LuceneSnapshottedSystem();

    private final String psqlContactAddress;

    public LuceneStorageSystemFactory(String psqlContactAddress) throws IOException {
        super("lucene", wrapper, ConstantsMAPP.LUCENE_LISTEN_PORT, storageSystem -> {
            var consumer = new LoopingConsumer(
                storageSystem.fullName,
                ConstantsMAPP.TEST_KAFKA_ADDRESS,
                ConstantsMAPP.KAFKA_TOPIC,
                storageSystem.classMap);

            consumer.moveAllToLatest();
            consumer.subscribe(storageSystem::kafkaActionHandler);
            Executors.newFixedThreadPool(1).submit(consumer::listenBlockingly);
        });
        this.psqlContactAddress = psqlContactAddress;

        wrapper.deleteAllMessages();
    }

    public LuceneStorageSystemFactory(String psqlContactAddress,
                                      Consumer<JointStorageSystem<IndexReader>> bootstrapProcedure) throws IOException {
        super("lucene", new LuceneSnapshottedSystem(), ConstantsMAPP.LUCENE_LISTEN_PORT, bootstrapProcedure);
        this.psqlContactAddress = psqlContactAddress;

        wrapper.deleteAllMessages();
    }

    @Override
    public JointStorageSystem<IndexReader> simpleOlep() {
        return new JointStorageSystemBuilder<>("lucene simple olep", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVERSATION
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // SEARCH MESSAGE
            .registerAction(new ActionBase<>(RequestSearchMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            .registerAction(new ActionBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
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
    JointStorageSystem<IndexReader> serReads() {
        return new JointStorageSystemBuilder<>("lucene ser reads", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVERSATION THREAD
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // SEARCH MESSAGE
            .registerAction(new ActionBase<>(RequestSearchMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            .registerAction(new ActionBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
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
    JointStorageSystem<IndexReader> sdRequestNoSession() {
        return new JointStorageSystemBuilder<>("lucene sd no session", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVERSATION THREAD
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // SEARCH MESSAGE
            .registerAction(new ActionBase<>(RequestSearchMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // SEARCH AND DETAILS
            .registerAction(new ActionBase<>(RequestSearchAndGetDetails.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    RequestSearchAndGetDetails requestSearchMessage = (RequestSearchAndGetDetails) request;
                    ResponseSearchMessage responseSearchMessage = wrapper.searchMessage(snapshot,
                        requestSearchMessage);

                    long idToLookFor = responseSearchMessage.getOccurrences().size() == 0
                        ? -1
                        : responseSearchMessage.getOccurrences().get(0);
                    var nextRequest = new RequestMessageDetails(request.getResponseAddress(), idToLookFor);

                    self.nextHopContact(psqlContactAddress, request, nextRequest);

                    return Response.CONFIRMATION;
                }
            })
            // SLEEP 1
            .registerAction(new ActionBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
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
    JointStorageSystem<IndexReader> sdRequestSeparateSession() {
        return new JointStorageSystemBuilder<>("lucene SD WITH session", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVERSATION THREAD
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestSearchMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // SEARCH AND DETAILS
            .registerAction(new ActionBase<>(RequestSearchAndGetDetails.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    RequestSearchAndGetDetails requestSearchMessage = (RequestSearchAndGetDetails) request;
                    ResponseSearchMessage responseSearchMessage = wrapper.searchMessage(snapshot, requestSearchMessage);

                    long idToLookFor = responseSearchMessage.getOccurrences().size() == 0
                        ? -1
                        : responseSearchMessage.getOccurrences().get(0);
                    var nextRequest = new RequestMessageDetails(request.getResponseAddress(), idToLookFor);

                    self.nextHopContact(psqlContactAddress, request, nextRequest);

                    return Response.CONFIRMATION; // Still sends a confirmation even though the full query hasn't finished yet
                }
            })
            .registerAction(new ActionBase<>(RequestSleep1.class, -1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
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
    JointStorageSystem<IndexReader> concurReads() {
        return new JointStorageSystemBuilder<>("lucene SD WITH session", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    return Response.CONFIRMATION;
                }
            })
            // DELETE CONVERSATION THREAD
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteConvoThread((RequestDeleteConversation) request);
                    return Response.CONFIRMATION;
                }
            })

            // DELETE ALL MESSAGES
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            // SEARCH MESSAGE
            .registerAction(new ActionBase<>(RequestSearchMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    return new Response(dbResponse);
                }
            })
            // SEARCH AND DETAILS
            .registerAction(new ActionBase<>(RequestSearchAndGetDetails.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<IndexReader> self,
                                     IndexReader snapshot
                ) {
                    RequestSearchAndGetDetails requestSearchMessage = (RequestSearchAndGetDetails) request;
                    ResponseSearchMessage responseSearchMessage = wrapper.searchMessage(snapshot, requestSearchMessage);

                    long idToLookFor = responseSearchMessage.getOccurrences().size() == 0
                        ? -1
                        : responseSearchMessage.getOccurrences().get(0);
                    var nextRequest = new RequestMessageDetails(request.getResponseAddress(), idToLookFor);

                    self.nextHopContact(psqlContactAddress, request, nextRequest);

                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestSleep1.class, 0) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
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
