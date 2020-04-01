import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import io.vavr.Tuple2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.LogManager;

public class EntryPoint {
    private static void makeItDance(LoadFaker loadFaker,
                                    Function<StorageSystemFactory, JointStorageSystem> factoryStrategy,
                                    List<Tuple2<String, String>> httpFavoursList) {
        String selfAddress = "192.168.1.50";
        String psqlAddress = String.format("http://localhost:%d", Constants.PSQL_LISTEN_PORT);

        try (var psqlFactory = new PsqlStorageSystemsFactory(Constants.PSQL_LISTEN_PORT);
            var luceneFactory = new LuceneStorageSystemFactory(psqlAddress + "/psql/contact");

             var ignored = factoryStrategy.apply(psqlFactory);
             var ignored1 = factoryStrategy.apply(luceneFactory);

             var storageApi = new StorageAPI(
                 KafkaUtils.createProducer(Constants.TEST_KAFKA_ADDRESS, "StorageApi"),
                 new HttpStorageSystem(
                     "StorageAPI",
                     HttpUtils.initHttpServer(Constants.STORAGEAPI_PORT)),
                 Constants.KAFKA_TOPIC, selfAddress,
                 httpFavoursList)) {

            fakeWithLoad(loadFaker, storageApi);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void fakeWithLoad(LoadFaker loadFaker, StorageAPI storageAPI) {
        while (true) {
            loadFaker.nextRequest(storageAPI);
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("HELLO!");

        // Log to a file
        LogManager.getLogManager().reset();
//        Logger.getLogger("").addHandler(new FileHandler("mylog.txt"));

        ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(Constants.METRIC_REGISTRY)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        consoleReporter. start(1, TimeUnit.SECONDS);

        Graphite graphite = new Graphite(new InetSocketAddress("localhost", 2003));
        GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(Constants.METRIC_REGISTRY)
            .prefixedWith("graphite")
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .filter(MetricFilter.ALL)
            .build(graphite);
        graphiteReporter.start(1, TimeUnit.SECONDS);

        ProportionsLoadFaker littleGetAllLoadFaker = new ProportionsLoadFaker(1_000, 1, Map.of(
            LoadFaker.Events.POST_MESSAGE, 0.24,
            LoadFaker.Events.GET_MESSAGE_DETAILS, 0.24,
            LoadFaker.Events.SEARCH_MESSAGES, 0.24,
            LoadFaker.Events.SEARCH_AND_DETAILS, 0.24,
            LoadFaker.Events.GET_ALL_MESSAGES, 0.04
        ));

        ProportionsLoadFaker noSDsLittleGetAll = new ProportionsLoadFaker(1_000, 1, Map.of(
            LoadFaker.Events.POST_MESSAGE, 0.30,
            LoadFaker.Events.GET_MESSAGE_DETAILS, 0.30,
            LoadFaker.Events.SEARCH_MESSAGES, 0.30,
            LoadFaker.Events.GET_ALL_MESSAGES, 0.10
        ));

        ProportionsLoadFaker noGetAllLoadFaker = new ProportionsLoadFaker(1_000, 1, Map.of(
            LoadFaker.Events.POST_MESSAGE, 0.25,
            LoadFaker.Events.GET_MESSAGE_DETAILS, 0.25,
            LoadFaker.Events.SEARCH_MESSAGES, 0.25,
            LoadFaker.Events.SEARCH_AND_DETAILS, 0.25));

        ProportionsLoadFaker noGetAllNoSDLoadFaker = new ProportionsLoadFaker(1_000, 1, Map.of(
            LoadFaker.Events.POST_MESSAGE, 0.34,
            LoadFaker.Events.GET_MESSAGE_DETAILS, 0.33,
            LoadFaker.Events.SEARCH_MESSAGES, 0.33));

        makeItDance(noGetAllNoSDLoadFaker, (StorageSystemFactory::concurReads),
            List.of());
    }
}
