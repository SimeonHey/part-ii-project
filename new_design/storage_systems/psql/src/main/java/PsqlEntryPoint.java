import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PsqlEntryPoint {
    public static void main(String[] args) throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        PsqlStorageSystemsFactory.simpleOlep(executorService);
    }
}
