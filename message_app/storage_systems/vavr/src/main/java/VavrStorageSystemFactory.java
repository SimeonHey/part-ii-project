import io.vavr.Tuple2;
import io.vavr.collection.HashMap;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class VavrStorageSystemFactory extends StorageSystemFactory<HashMap<Tuple2<String, String>, Integer>> {
    private final static VavrSnapshottedDatabase wrapper = new VavrSnapshottedDatabase();

    public VavrStorageSystemFactory(int httpListenPort)
        throws IOException {
        super("vavr", wrapper, httpListenPort, (storageSystem) -> {
            var consumer = new LoopingConsumer(
                storageSystem.fullName,
                ConstantsMAPP.TEST_KAFKA_ADDRESS,
                ConstantsMAPP.KAFKA_TOPIC,
                storageSystem.classMap);
            consumer.moveAllToLatest();
            consumer.subscribe(storageSystem::kafkaActionHandler);
            Executors.newFixedThreadPool(1).submit(consumer::listenBlockingly);
        });
    }

    public VavrStorageSystemFactory(int httpListenPort,
                                    Consumer<StorageSystem<HashMap<Tuple2<String, String>, Integer>>> bootstrapProcedure)
        throws IOException {
        super("vavr", wrapper, httpListenPort, bootstrapProcedure);
    }

    @Override
    StorageSystem<HashMap<Tuple2<String, String>, Integer>> simpleOlep() {
        return new StorageSystemBuilder<>("vavr simple olep", this.httpStorageSystem, wrapper,
            this.bootstrapProcedure)
            .registerAction(new ActionBase<>(RequestPostMessage.class, false) {
                @Override
                Response handleEvent(EventBase request,
                                     StorageSystem<HashMap<Tuple2<String, String>, Integer>> self,
                                     HashMap<Tuple2<String, String>, Integer> snapshot) {
                    var postRequest = (RequestPostMessage) request;
                    wrapper.increaseMessagesForPair(postRequest.getMessage().getSender(),
                        postRequest.getMessage().getRecipient());

                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, false) {
                @Override
                Response handleEvent(EventBase request, StorageSystem<HashMap<Tuple2<String, String>, Integer>> self,
                                     HashMap<Tuple2<String, String>, Integer> snapshot) {
                    var deleteRequest = (RequestDeleteConversation) request;
                    wrapper.deleteConversation(deleteRequest.getUser1(), deleteRequest.getUser2());
                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, false) {
                @Override
                Response handleEvent(EventBase request, StorageSystem<HashMap<Tuple2<String, String>, Integer>> self,
                                     HashMap<Tuple2<String, String>, Integer> snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestGetTotalNumberOfMessages.class, false) {
                @Override
                Response handleEvent(EventBase request,
                                     StorageSystem<HashMap<Tuple2<String, String>, Integer>> self,
                                     HashMap<Tuple2<String, String>, Integer> snapshot) {
                    var numberRequest = (RequestGetTotalNumberOfMessages) request;
                    return new Response(
                        wrapper.getTotalMessages(snapshot, numberRequest.getOfUser1(), numberRequest.getOfUser2()));
                }
            }).buildAndRun();
    }

    @Override
    StorageSystem<HashMap<Tuple2<String, String>, Integer>> serReads() {
        return simpleOlep();
    }

    @Override
    StorageSystem<HashMap<Tuple2<String, String>, Integer>> sdRequestNoSession() {
        return simpleOlep();
    }

    @Override
    StorageSystem<HashMap<Tuple2<String, String>, Integer>> sdRequestSeparateSession() {
        return simpleOlep();
    }

    @Override
    StorageSystem<HashMap<Tuple2<String, String>, Integer>> concurReads() {
        return new StorageSystemBuilder<>("vavr concur reads", this.httpStorageSystem, wrapper,
            this.bootstrapProcedure)
            .registerAction(new ActionBase<>(RequestPostMessage.class, false) {
                @Override
                Response handleEvent(EventBase request,
                                     StorageSystem<HashMap<Tuple2<String, String>, Integer>> self,
                                     HashMap<Tuple2<String, String>, Integer> snapshot) {
                    var postRequest = (RequestPostMessage) request;
                    wrapper.increaseMessagesForPair(postRequest.getMessage().getSender(),
                        postRequest.getMessage().getRecipient());

                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestDeleteConversation.class, false) {
                @Override
                Response handleEvent(EventBase request, StorageSystem<HashMap<Tuple2<String, String>, Integer>> self,
                                     HashMap<Tuple2<String, String>, Integer> snapshot) {
                    var deleteRequest = (RequestDeleteConversation) request;
                    wrapper.deleteConversation(deleteRequest.getUser1(), deleteRequest.getUser2());
                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestDeleteAllMessages.class, false) {
                @Override
                Response handleEvent(EventBase request, StorageSystem<HashMap<Tuple2<String, String>, Integer>> self,
                                     HashMap<Tuple2<String, String>, Integer> snapshot) {
                    wrapper.deleteAllMessages();
                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestGetTotalNumberOfMessages.class, true) {
                @Override
                Response handleEvent(EventBase request,
                                     StorageSystem<HashMap<Tuple2<String, String>, Integer>> self,
                                     HashMap<Tuple2<String, String>, Integer> snapshot) {
                    var numberRequest = (RequestGetTotalNumberOfMessages) request;
                    return new Response(wrapper.getTotalMessages(snapshot, numberRequest.getOfUser1(),
                        numberRequest.getOfUser2()));
                }
            }).buildAndRun();
    }
}
