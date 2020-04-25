import io.vavr.collection.HashMap;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class VavrStorageSystemFactory extends StorageSystemFactory<HashMap<String, Integer>> {
    private final static VavrSnapshottedSystem wrapper = new VavrSnapshottedSystem();

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
                                    Consumer<JointStorageSystem<HashMap<String, Integer>>> bootstrapProcedure)
        throws IOException {
        super("vavr", wrapper, httpListenPort, bootstrapProcedure);
    }

    @Override
    JointStorageSystem<HashMap<String, Integer>> simpleOlep() {
        return new JointStorageSystemBuilder<>("vavr simple olep", this.httpStorageSystem, wrapper,
            this.bootstrapProcedure)
            .registerAction(new ActionBase<>(RequestPostMessage.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<HashMap<String, Integer>> self,
                                     HashMap<String, Integer> snapshot) {
                    String recipient = ((RequestPostMessage) request).getMessage().getRecipient();
                    wrapper.handleMessagePosted(recipient);

                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestConvoMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request, JointStorageSystem<HashMap<String, Integer>> self,
                                     HashMap<String, Integer> snapshot) {
                    wrapper.handleConvoMessagesRequest((RequestConvoMessages) request);
                    return Response.CONFIRMATION;
                }
            })
            .registerAction(new ActionBase<>(RequestGetUnreadMessages.class, -1) {
                @Override
                Response handleEvent(EventBase request,
                                     JointStorageSystem<HashMap<String, Integer>> self,
                                     HashMap<String, Integer> snapshot) {
                    String ofUser = ((RequestGetUnreadMessages) request).getOfUser();
                    return new Response(wrapper.getUnreadMessages(ofUser));
                }
            }).build();
    }

    @Override
    JointStorageSystem<HashMap<String, Integer>> serReads() {
        return simpleOlep();
    }

    @Override
    JointStorageSystem<HashMap<String, Integer>> sdRequestNoSession() {
        return simpleOlep();
    }

    @Override
    JointStorageSystem<HashMap<String, Integer>> sdRequestSeparateSession() {
        return simpleOlep();
    }

    @Override
    JointStorageSystem<HashMap<String, Integer>> concurReads() {
        return simpleOlep();
    }
}
