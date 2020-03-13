import java.util.concurrent.Executors;

public class EntryPoint {
    public static void main(String[] args) {
        try (var ignored = new PsqlStorageSystemsFactory(Executors.newFixedThreadPool(1)).serReads();
             var ignored1 = new LuceneStorageSystemFactory(Executors.newFixedThreadPool(1)).serReads();
             var storageApi = new StorageAPI(
                 KafkaUtils.createProducer(Constants.KAFKA_ADDRESS, "StorageApi"),
                 new HttpStorageSystem(
                     "StorageAPI",
                     HttpUtils.initHttpServer(Constants.STORAGEAPI_PORT)),
                 Constants.KAFKA_TOPIC)) {

            for (int i = 0; i < 10; i++) {
                storageApi.postMessage(new Message(String.valueOf(i), String.valueOf(i)));
            }

            System.out.println(storageApi.searchMessage("9"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("PLEASE TERMINATE MANUALLY");
    }
}
