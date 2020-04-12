import io.vavr.collection.HashMap;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class VavrStorageSystemFactory extends StorageSystemFactory<HashMap<String, Integer>> {
    private final static VavrSnapshottedWrapper wrapper = new VavrSnapshottedWrapper();

    public VavrStorageSystemFactory(int httpListenPort)
        throws IOException {
        super("vavr", wrapper, httpListenPort, (storageSystem) -> {
            var consumer = LoopingConsumer.fresh(
                storageSystem.fullName,
                ConstantsMAPP.TEST_KAFKA_ADDRESS,
                storageSystem.classMap);
            consumer.moveAllToLatest();
            consumer.subscribe(storageSystem::kafkaServiceHandler);
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
            .registerKafkaService(new ServiceBase<>(RequestPostMessage.class, -1) {
                @Override
                Object handleRequest(BaseEvent request,
                                   JointStorageSystem<HashMap<String, Integer>> self,
                                   HashMap<String, Integer> snapshot) {
                    String recipient = ((RequestPostMessage) request).getRecepient();
                    wrapper.postMessage(recipient);

                    return null;

//                    responseCallback.accept(new ConfirmationResponse(self.fullName, request.getEventType()));
                }
            })
            .registerHttpService(new ServiceBase<>(RequestAllMessages.class, -1) {
                @Override
                Object handleRequest(BaseEvent request, JointStorageSystem<HashMap<String
                    , Integer>> self, HashMap<String, Integer> snapshot) {
                    wrapper.getAllMessages((RequestAllMessages) request);

//                    responseCallback.accept(new ConfirmationResponse(self.fullName, request.getEventType()));
                    return null;
                }
            })
            .registerHttpService(new ServiceBase<>(RequestGetUnreadMessages.class, -1) {
                @Override
                Object handleRequest(BaseEvent request,
                                   JointStorageSystem<HashMap<String, Integer>> self,
                                   HashMap<String, Integer> snapshot) {
                    String ofUser = ((RequestGetUnreadMessages) request).getOfUser();
                    return wrapper.getUnreadMessages(ofUser);
                }
            }).build();
    }

    @Override
    JointStorageSystem<HashMap<String, Integer>> serReads() {
        return null;
    }

    @Override
    JointStorageSystem<HashMap<String, Integer>> sdRequestNoSession() {
        return null;
    }

    @Override
    JointStorageSystem<HashMap<String, Integer>> sdRequestSeparateSession() {
        return null;
    }

    @Override
    JointStorageSystem<HashMap<String, Integer>> concurReads() {
        return null;
    }
}
