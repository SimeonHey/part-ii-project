import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.logging.Logger;

public class StorageAPIEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(StorageAPIEntryPoint.class.getName());

    public static void main(String[] args) throws IOException {
        LOGGER.info("Starting StorageAPI with params " + Arrays.toString(args));
        StorageAPIUtils.StorageAPIInitArgs initArgs = StorageAPIUtils.StorageAPIInitArgs.defaultTestValues();
        StorageAPI storageAPI = StorageAPIUtils.initFromArgsForTests(initArgs);

        // Take user commands and perform actions
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter query:");
            String[] line;

            try {
                line = scanner.nextLine().split(" ");
            } catch (NoSuchElementException e) {
                LOGGER.info("End of user input.. breaking out of the loop");
                break;
            }

            LOGGER.info("Got " + Arrays.toString(line));

            switch (line[0]) {
                case "post":
                    storageAPI.postMessage(new Message(line[1], line[2]));
                    break;
                case "search":
                    ResponseSearchMessage responseSearchMessage =
                        storageAPI.searchMessage(line[1]);
                    System.out.println("Search response: " + responseSearchMessage);
                    break;
                case "details":
                    ResponseMessageDetails responseMessageDetails =
                        storageAPI.messageDetails(Long.valueOf(line[1]));
                    System.out.println("Message details: " + responseMessageDetails);
                    break;
                case "all":
                    ResponseAllMessages responseAllMessages =
                        storageAPI.allMessages();
                    System.out.println("All messages: " + responseAllMessages);
                    break;
                case "clean":
                    storageAPI.deleteAllMessages();
                    System.out.println("Deleted all messages");
                    break;
                /*case "sd":
                    ResponseMessageDetails responseMessageDetails1 =
                        storageAPI.searchAndDetails(line[1]);
                    System.out.println("Message details: " + responseMessageDetails1);
                    break;*/
                default:
                    System.out.println("Couldn't catch that");
            }
        }
    }
}
