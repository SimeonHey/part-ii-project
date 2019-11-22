import java.awt.*;
import java.io.IOException;
import java.util.logging.Logger;

public class MessageAppEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(MessageAppEntryPoint.class.getName());

    public static void main(String[] args) throws InterruptedException {
        StorageAPI storageAPI =
            StorageAPIUtils.initFromArgs(args[0], args[1], args[2], args[3]);

        EventQueue.invokeLater(() -> {
            var simeon = new FrontEnd("Simeon");
            var martin = new FrontEnd("Martin");

            simeon.addPostCallback(storageAPI::postMessage);
            simeon.addRefreshCallback(() -> {
                ResponseAllMessages allMessages;
                try {
                    allMessages = storageAPI.allMessages();
                } catch (IOException e) {
                    LOGGER.info("Error when getting all messages: " + e);
                    throw new RuntimeException(e);
                }
                simeon.setMessages(allMessages.getMessages());
            });

            martin.addPostCallback(storageAPI::postMessage);
            martin.addRefreshCallback(() -> {
                ResponseAllMessages allMessages;
                try {
                    allMessages = storageAPI.allMessages();
                } catch (IOException e) {
                    LOGGER.info("Error when getting all messages: " + e);
                    throw new RuntimeException(e);
                }
                martin.setMessages(allMessages.getMessages());
            });
        });
    }
}
