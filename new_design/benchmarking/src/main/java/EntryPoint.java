import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

public class EntryPoint {
    private static void makeItDance(LoadFaker loadFaker) {
        String selfAddress = "192.168.1.12";
        String psqlAddress = "http://localhost:1234"; //

        try (var psqlFactory = new PsqlStorageSystemsFactory(LoopingConsumer.fresh("psql",
            Constants.TEST_KAFKA_ADDRESS), 1234);
            var luceneFactory = new LuceneStorageSystemFactory(LoopingConsumer.fresh("lucene",
            Constants.TEST_KAFKA_ADDRESS), psqlAddress + "/psql/contact");
             var ignored = psqlFactory.concurReads();
             var ignored1 = luceneFactory.concurReads();
             var storageApi = new StorageAPI(
                 KafkaUtils.createProducer(Constants.TEST_KAFKA_ADDRESS, "StorageApi"),
                 new HttpStorageSystem(
                     "StorageAPI",
                     HttpUtils.initHttpServer(Constants.STORAGEAPI_PORT)),
                 Constants.KAFKA_TOPIC, selfAddress)) {

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

    public static void main(String[] args) {
        System.out.println("HELLO!");
        LogManager.getLogManager().reset();

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

        makeItDance(new NonDeleteUniformLoadFaker());
    }
}
