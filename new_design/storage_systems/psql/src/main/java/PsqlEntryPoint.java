import java.io.IOException;
import java.util.concurrent.Executors;

public class PsqlEntryPoint {
    public static void main(String[] args) throws IOException {
        var psqlFactory = new PsqlStorageSystemsFactory(LoopingConsumer.fresh("psql"));
        psqlFactory.simpleOlep();
        psqlFactory.listenBlockingly(Executors.newFixedThreadPool(1));
    }
}
