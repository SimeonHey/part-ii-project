import java.util.concurrent.Executors;

public class EntryPoint {
    public static void main(String[] args) {

        String selfAddress = "192.168.1.12";

        var producer = KafkaUtils.createProducer(Constants.TEST_KAFKA_ADDRESS, "JUST TESTING");
        KafkaUtils.produceMessage(producer, Constants.KAFKA_TOPIC,
            RequestNOP.toStupidStreamObject(Constants.NO_RESPONSE));

        try (var luceneFactory = new LuceneStorageSystemFactory(LoopingConsumer.fresh("lucene",
            Constants.TEST_KAFKA_ADDRESS),
                 "http://192.168.1.8:1234/psql/contact");
//             var ignored = psqlFactory.serReads();
             var ignored1 = luceneFactory.concurReads();
             var storageApi = new StorageAPI(
                 KafkaUtils.createProducer(Constants.TEST_KAFKA_ADDRESS, "StorageApi"),
                 new HttpStorageSystem(
                     "StorageAPI",
                     HttpUtils.initHttpServer(Constants.STORAGEAPI_PORT)),
                 Constants.KAFKA_TOPIC, selfAddress)) {

//            psqlFactory.listenBlockingly(Executors.newFixedThreadPool(1));
            luceneFactory.listenBlockingly(Executors.newFixedThreadPool(1));

            int cnt = 0;
            while (true) {
                for (int i = 0; i < 1000; i++, cnt++) {
                    storageApi.postMessage(new Message(String.valueOf(cnt), String.valueOf(cnt)));
                }

                System.out.println("Waiting for THE QUERY");
                System.out.println(storageApi.searchAndDetails(String.valueOf(cnt-1)));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
