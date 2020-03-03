import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class LuceneStorageSystemFactory extends StorageSystemFactory<IndexReader> {
    private static final Logger LOGGER = Logger.getLogger(LuceneStorageSystemFactory.class.getName());

    public LuceneStorageSystemFactory(ExecutorService executorService) throws IOException {
        super("Lucene", executorService, new LuceneSnapshottedWrapper(), Constants.LUCENE_LISTEN_PORT);
    }

    @Override
    public JointStorageSystem<IndexReader> simpleOlep() {
        var ss = new JointStorageSystem<>("LUCENE simple olep", loopingKafka, httpStorageSystem,
            snapshottedWrapper)
            // POST MESSAGE
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.POST_MESSAGE, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    wrapper.postMessage(RequestPostMessage.fromStupidStreamObject(request));
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), null);
                    responseCallback.accept(response);
                }
            })
            // DELETE ALL MESSAGES
            .registerKafkaService(new ServiceBase<>(StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    wrapper.deleteAllMessages();
                    var response = new MultithreadedResponse(request.getResponseAddress().getChannelID(), null);
                    responseCallback.accept(response);
                }
            })
            // GET ALL MESSAGES
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    LOGGER.info("Lucene received a get all messages request and does nothing");
                }
            })
            // GET MESSAGE DETAILS
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    LOGGER.info("Lucene received a get message details request and does nothing");
                }
            })
            // SEARCH MESSAGE
            .registerHttpService(new ServiceBase<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, false) {
                @Override
                void handleRequest(StupidStreamObject request,
                                   WrappedSnapshottedStorageSystem<IndexReader> wrapper,
                                   Consumer<MultithreadedResponse> responseCallback) {
                    var dbResponse = wrapper.searchMessage(wrapper.getDefaultSnapshot(),
                        RequestSearchMessage.fromStupidStreamObject(request));
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
                                   Consumer<MultithreadedResponse> responseCallback) {
                    LOGGER.info("Lucene simple olep received NOP");
//                    responseCallback.accept(new StupidStreamObject(StupidStreamObject.ObjectType.NOP,
//                        Constants.NO_RESPONSE));
                }
            });

        this.executorService.submit(loopingKafka::listenBlockingly);
        return ss;
    }
}
