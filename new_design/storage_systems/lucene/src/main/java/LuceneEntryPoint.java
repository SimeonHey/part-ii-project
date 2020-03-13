import java.io.IOException;
import java.util.concurrent.Executors;

public class LuceneEntryPoint {
    public static void main(String[] args) throws IOException {
        var luceneFactory = new LuceneStorageSystemFactory(LoopingConsumer.fresh("lucene"));
        luceneFactory.simpleOlep();
        luceneFactory.listenBlockingly(Executors.newFixedThreadPool(1));
    }
}
