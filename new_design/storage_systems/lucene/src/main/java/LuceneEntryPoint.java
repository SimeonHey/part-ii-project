import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LuceneEntryPoint {
    public static void main(String[] args) throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        new LuceneStorageSystemFactory(executorService).simpleOlep();
    }
}
