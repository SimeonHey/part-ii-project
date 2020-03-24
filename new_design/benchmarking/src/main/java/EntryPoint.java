import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import io.vavr.Tuple2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class EntryPoint {
    private static void makeItDance(LoadFaker loadFaker,
                                    Function<StorageSystemFactory, JointStorageSystem> factoryStrategy,
                                    List<Tuple2<StupidStreamObject.ObjectType, String>> httpFavoursList,
                                    int outstandingFavoursLimit) {
        String selfAddress = "192.168.1.50";
        String psqlAddress = String.format("http://localhost:%d", Constants.PSQL_LISTEN_PORT);

        try (var psqlFactory = new PsqlStorageSystemsFactory(LoopingConsumer.fresh("psql",
            Constants.TEST_KAFKA_ADDRESS), Constants.PSQL_LISTEN_PORT);
            var luceneFactory = new LuceneStorageSystemFactory(LoopingConsumer.fresh("lucene",
            Constants.TEST_KAFKA_ADDRESS), psqlAddress + "/psql/contact");

             var ignored = factoryStrategy.apply(psqlFactory);
             var ignored1 = factoryStrategy.apply(luceneFactory);

             var storageApi = new StorageAPI(
                 KafkaUtils.createProducer(Constants.TEST_KAFKA_ADDRESS, "StorageApi"),
                 new HttpStorageSystem(
                     "StorageAPI",
                     HttpUtils.initHttpServer(Constants.STORAGEAPI_PORT)),
                 Constants.KAFKA_TOPIC, selfAddress,
                 httpFavoursList,
                 outstandingFavoursLimit)) {

            psqlFactory.listenBlockingly(Executors.newFixedThreadPool(1));
            luceneFactory.listenBlockingly(Executors.newFixedThreadPool(1));

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
//        LogManager.getLogManager().reset();
//        Logger.getLogger("").addHandler(new FileHandler("mylog.txt"));

        ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(Constants.METRIC_REGISTRY)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        consoleReporter.start(1, TimeUnit.SECONDS);

        Graphite graphite = new Graphite(new InetSocketAddress("localhost", 2003));
        GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(Constants.METRIC_REGISTRY)
            .prefixedWith("graphite")
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .filter(MetricFilter.ALL)
            .build(graphite);
        graphiteReporter.start(1, TimeUnit.SECONDS);

        makeItDance(new NoSDUniformLoadFaker(99_000, 1), (StorageSystemFactory::concurSchedule),
            List.of(/*
                new Tuple2<>(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, Constants.TEST_PSQL_REQUEST_ADDRESS),
                new Tuple2<>(StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS, Constants.TEST_PSQL_REQUEST_ADDRESS),
                new Tuple2<>(StupidStreamObject.ObjectType.SEARCH_MESSAGES, Constants.TEST_LUCENE_REQUEST_ADDRESS)*/),
            100 /*not used atm*/);
    }
}
