import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import io.vavr.Tuple2;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class EntryPoint {
    private static void makeItDance(LoadFaker loadFaker,
                                    Function<StorageSystemFactory, JointStorageSystem> factoryStrategy,
                                    List<Tuple2<String, List<String>>> httpFavoursList) {
        String selfAddress = "192.168.1.50";
        String psqlAddress = String.format("http://localhost:%d", ConstantsMAPP.PSQL_LISTEN_PORT);

        System.out.println("Initializing stuff:");
        try (var psqlFactory = new PsqlStorageSystemsFactory(ConstantsMAPP.PSQL_LISTEN_PORT);
             var luceneFactory = new LuceneStorageSystemFactory(psqlAddress + "/psql/contact");
             var vavrFactory = new VavrStorageSystemFactory(ConstantsMAPP.VAVR_LISTEN_PORT);

             var ignored = factoryStrategy.apply(psqlFactory);
             var ignored1 = factoryStrategy.apply(luceneFactory);
             var ignored2 = factoryStrategy.apply(vavrFactory);

             var storageApi = new PolyglotAPI(
                 KafkaUtils.createProducer(ConstantsMAPP.TEST_KAFKA_ADDRESS, "StorageApi"),
                 new HttpStorageSystem(
                     "StorageAPI",
                     HttpUtils.initHttpServer(ConstantsMAPP.STORAGEAPI_PORT)),
                 ConstantsMAPP.KAFKA_TOPIC, selfAddress,
                 httpFavoursList)) {

            System.out.println("Initialized stuff! Faking load...");
            fakeWithLoad(loadFaker, storageApi);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void fakeWithLoad(LoadFaker loadFaker, PolyglotAPI polyglotAPI) throws InterruptedException, ExecutionException {
        long targetRatePerSecond = 10;
        long nanosPerSec = 1_000_000_000;
        long nanosPerRequest = nanosPerSec / targetRatePerSecond;
//        long millisPerRequest = nanosPerRequest / 1_000_000;
//        long toSleep = millisPerRequest / 5;
        long nanosPerChange = nanosPerSec * 120L; // Each 60 seconds

        long lastRatechangeTime = System.nanoTime();
        long lastSentTime = System.nanoTime();

        SettableGauge<Long> rateMetric = new SettableGauge<>();
        Constants.METRIC_REGISTRY.register("polyglotAPI.controlledRate", rateMetric);
        rateMetric.setValue(targetRatePerSecond);

        while (true) {
            long elapsedLastRequest = System.nanoTime() - lastSentTime;

            if (elapsedLastRequest > nanosPerRequest) {
                loadFaker.nextRequest(polyglotAPI);
                lastSentTime += nanosPerRequest;
//                System.out.println("Sent at " + lastSentTime);
            }

            long elapsedRatechangeTime = System.nanoTime() - lastRatechangeTime;
            if (elapsedRatechangeTime > nanosPerChange) {
                lastRatechangeTime = System.nanoTime();
                targetRatePerSecond += 10;
                nanosPerRequest = nanosPerSec / targetRatePerSecond;
                polyglotAPI.handleRequest(
                    new RequestDeleteAllMessages(new Addressable(polyglotAPI.getResponseAddress()))).get();
                rateMetric.setValue(targetRatePerSecond);
                System.out.println("Changed rate to " + targetRatePerSecond + " at " + lastRatechangeTime);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("HELLO!");

        // Log to a file
//        LogManager.getLogManager().reset();
//        Logger.getLogger("").addHandler(new FileHandler("mylog.txt"));

        ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(Constants.METRIC_REGISTRY)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
//        consoleReporter. start(1, TimeUnit.SECONDS);

        Graphite graphite = new Graphite(new InetSocketAddress("localhost", 2003));
        GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(Constants.METRIC_REGISTRY)
            .prefixedWith("graphite")
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .filter(MetricFilter.ALL)
            .build(graphite);
        graphiteReporter.start(1, TimeUnit.SECONDS);

        String mainDir = "/home/simeon/Documents/Cambridge/project/part-ii-project/dontgitme";
        String subFolder = String.valueOf(System.currentTimeMillis());
        File mine = new File(mainDir + "/" + subFolder);
        mine.mkdir();
        CsvReporter csvReporter = CsvReporter.forRegistry(Constants.METRIC_REGISTRY)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(mine);
        csvReporter.start(1, TimeUnit.SECONDS);

        makeItDance(new UniformLoadFaker(1_000, 1), (StorageSystemFactory::sdRequestSeparateSession),
            List.of(/*new Tuple2<>(RequestAllMessages.class.getName(), ConstantsMAPP.TEST_PSQL_REQUEST_ADDRESS),
                new Tuple2<>(RequestMessageDetails.class.getName(), ConstantsMAPP.TEST_PSQL_REQUEST_ADDRESS),
                new Tuple2<>(RequestSearchMessage.class.getName(), ConstantsMAPP.TEST_LUCENE_REQUEST_ADDRESS)*/));
    }
}
