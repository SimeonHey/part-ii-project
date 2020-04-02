import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

public class MessageAppEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(MessageAppEntryPoint.class.getName());

    public static void main(String[] args) throws InterruptedException, IOException {
        LOGGER.info("Initializing a storage api with arguments " + Arrays.toString(args));

        /*
        StorageAPIUtils.StorageAPIInitArgs initArgs = new StorageAPIUtils.StorageAPIInitArgs(args);

        StorageAPI storageAPI =
            StorageAPIUtils.initFromArgsForTests(initArgs);

        LOGGER.info("Deleting all previous messages");
        storageAPI.deleteAllMessages();

        LOGGER.info("Initializing the UIs...");
        EventQueue.invokeLater(() -> {
            new FrontEnd("Simeon", storageAPI);
            new FrontEnd("Martin", storageAPI);
        });*/
    }
}
