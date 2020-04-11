import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class LuceneStorageSystemFactory extends StorageSystemFactory<IndexReader> {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystemFactory.class.getName());

    private static final LuceneSnapshottedWrapper wrapper = new LuceneSnapshottedWrapper();

    private final String psqlContactAddress;

    public LuceneStorageSystemFactory(String psqlContactAddress) throws IOException {
        super("lucene", wrapper, ConstantsMAPP.LUCENE_LISTEN_PORT, storageSystem -> {
            var consumer = LoopingConsumer.fresh(
                storageSystem.fullName,
                ConstantsMAPP.TEST_KAFKA_ADDRESS,
                storageSystem.classMap);
            consumer.moveAllToLatest();
            consumer.subscribe(storageSystem::kafkaServiceHandler);
            Executors.newFixedThreadPool(1).submit(consumer::listenBlockingly);
        });
        this.psqlContactAddress = psqlContactAddress;

        wrapper.deleteAllMessages();
    }

    public LuceneStorageSystemFactory(String psqlContactAddress,
                                      Consumer<JointStorageSystem<IndexReader>> bootstrapProcedure) throws IOException {
        super("lucene", new LuceneSnapshottedWrapper(), ConstantsMAPP.LUCENE_LISTEN_PORT, bootstrapProcedure);
        this.psqlContactAddress = psqlContactAddress;

        wrapper.deleteAllMessages();
    }

    @Override
    public JointStorageSystem<IndexReader> simpleOlep() {
        return new JointStorageSystemBuilder<>("lucene simple olep", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // SEARCH MESSAGE
            .registerHttpService(new ServiceBase<>(RequestSearchMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new ChanneledResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            .registerKafkaService(new ServiceBase<>(RequestSleep1.class, -1) {
                @Override
                void handleRequest(BaseEvent request, Consumer<ChanneledResponse> responseCallback, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }
                }
            })
            .registerKafkaService(new ServiceBase<>(RequestSleep1.class, -1) {
                @Override
                void handleRequest(BaseEvent request, Consumer<ChanneledResponse> responseCallback, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }
                }
            }).build();
    }

    @Override
    JointStorageSystem<IndexReader> serReads() {
        return new JointStorageSystemBuilder<>("lucene ser reads", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // SEARCH MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestSearchMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new ChanneledResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            .registerKafkaService(new ServiceBase<>(RequestSleep1.class, -1) {
                @Override
                void handleRequest(BaseEvent request, Consumer<ChanneledResponse> responseCallback, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }
                }
            }).build();
    }

    @Override
    JointStorageSystem<IndexReader> sdRequestNoSession() {
        return new JointStorageSystemBuilder<>("lucene sd no session", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // SEARCH MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestSearchMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new ChanneledResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(RequestSearchAndDetails.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    RequestSearchAndDetails requestSearchMessage = (RequestSearchAndDetails) request;
                    ResponseSearchMessage responseSearchMessage = wrapper.searchMessage(snapshot,
                        requestSearchMessage);

                    long idToLookFor = responseSearchMessage.getOccurrences().size() == 0
                        ? -1
                        : responseSearchMessage.getOccurrences().get(0);
                    var nextRequest = new RequestMessageDetails(request.getResponseAddress(), idToLookFor);

                    String serialized = ConstantsMAPP.gson.toJson(
                        new ChanneledResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), nextRequest));

                    try {
                        HttpUtils.sendHttpRequest(psqlContactAddress, serialized);
                    } catch (IOException e) {
                        LOGGER.warning("Error when trying to contact psql for next hop of the request");
                        throw new RuntimeException(e);
                    }
                }
            })
            .registerKafkaService(new ServiceBase<>(RequestSleep1.class, -1) {
                @Override
                void handleRequest(BaseEvent request, Consumer<ChanneledResponse> responseCallback, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }
                }
            }).build();
    }

    @Override
    JointStorageSystem<IndexReader> sdRequestSeparateSession() {
        return new JointStorageSystemBuilder<>("lucene SD WITH session", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            }).registerKafkaService(new ServiceBase<>(RequestSearchMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new ChanneledResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(RequestSearchAndDetails.class, 0) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    RequestSearchAndDetails requestSearchMessage = (RequestSearchAndDetails) request;
                    ResponseSearchMessage responseSearchMessage = wrapper.searchMessage(snapshot, requestSearchMessage);

                    long idToLookFor = responseSearchMessage.getOccurrences().size() == 0
                        ? -1
                        : responseSearchMessage.getOccurrences().get(0);
                    var nextRequest = new RequestMessageDetails(request.getResponseAddress(), idToLookFor);
                    LOGGER.info("Contacting PSQL with details request: " + nextRequest);

                    String serialized = ConstantsMAPP.gson.toJson(
                        new ChanneledResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), nextRequest));
                    try {
                        HttpUtils.sendHttpRequest(psqlContactAddress, serialized);
                    } catch (IOException e) {
                        LOGGER.warning("Error when trying to contact psql for next hop of the request");
                        throw new RuntimeException(e);
                    }
                }
            })
            .registerKafkaService(new ServiceBase<>(RequestSleep1.class, -1) {
                @Override
                void handleRequest(BaseEvent request, Consumer<ChanneledResponse> responseCallback, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }
                }
            }).build();
    }

    @Override
    JointStorageSystem<IndexReader> concurReads() {
        return new JointStorageSystemBuilder<>("lucene SD WITH session", httpStorageSystem,
            snapshottedWrapper, this.bootstrapProcedure)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.postMessage((RequestPostMessage) request);
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(RequestDeleteAllMessages.class, -1) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    wrapper.deleteAllMessages();
                    var response = new ChanneledResponse(self.shortName, request.getEventType(),
                        request.getResponseAddress().getChannelID(),
                        new ConfirmationResponse(self.fullName, request.getEventType()));
                    responseCallback.accept(response);
                }
            })// SEARCH MESSAGE
            .registerKafkaService(new ServiceBase<>(RequestSearchMessage.class, 0) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    var dbResponse = wrapper.searchMessage(snapshot,
                        (RequestSearchMessage) request);
                    LOGGER.info("Result from search: " + dbResponse);
                    responseCallback.accept(
                        new ChanneledResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), dbResponse)
                    );
                }
            })
            // SEARCH AND DETAILS
            .registerKafkaService(new ServiceBase<>(RequestSearchAndDetails.class, 0) {
                @Override
                void handleRequest(BaseEvent request,
                                   Consumer<ChanneledResponse> responseCallback,
                                   JointStorageSystem<IndexReader> self,
                                   IndexReader snapshot
                ) {
                    RequestSearchAndDetails requestSearchMessage = (RequestSearchAndDetails) request;
                    ResponseSearchMessage responseSearchMessage = wrapper.searchMessage(snapshot, requestSearchMessage);

                    long idToLookFor = responseSearchMessage.getOccurrences().size() == 0
                        ? -1
                        : responseSearchMessage.getOccurrences().get(0);
                    var nextRequest = new RequestMessageDetails(request.getResponseAddress(), idToLookFor);
                    LOGGER.info("Contacting PSQL with details request: " + nextRequest);

                    String serialized = ConstantsMAPP.gson.toJson(
                        new ChanneledResponse(self.shortName, request.getEventType(),
                            request.getResponseAddress().getChannelID(), nextRequest));
                    try {
                        HttpUtils.sendHttpRequest(psqlContactAddress, serialized);
                    } catch (IOException e) {
                        LOGGER.warning("Error when trying to contact psql for next hop of the request");
                        throw new RuntimeException(e);
                    }
                }
            })
            .registerKafkaService(new ServiceBase<>(RequestSleep1.class, 1) {
                @Override
                void handleRequest(BaseEvent request, Consumer<ChanneledResponse> responseCallback, JointStorageSystem<IndexReader> self, IndexReader snapshot) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Couldn't sleep!!@!");
                        throw new RuntimeException(e);
                    }
                }
            }).build();
    }
}
