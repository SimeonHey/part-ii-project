import java.awt.*;
import java.util.Arrays;
import java.util.logging.Logger;

public class MessageAppEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(MessageAppEntryPoint.class.getName());

    public static void main(String[] args) throws InterruptedException {
        LOGGER.info("Initializing a storage api with arguments " + Arrays.toString(args));

        StorageAPI storageAPI =
            StorageAPIUtils.initFromArgs(args[0], args[1], args[2], args[3]);

        LOGGER.info("Deleting all previous messages");
        storageAPI.deleteAllMessages();

        LOGGER.info("Initializing the UIs...");
        EventQueue.invokeLater(() -> {
            new FrontEnd("Simeon", storageAPI);
            new FrontEnd("Martin", storageAPI);
        });
    }
}
