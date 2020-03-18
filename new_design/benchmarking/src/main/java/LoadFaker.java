import java.util.Random;

public abstract class LoadFaker {
    Random random = new Random(22335577);
    private final int charsLimit = 3;
    private final int wordsLimit = 20;
    int currentRequests = 0;

    void callFromId(int id, StorageAPI storageAPI) {
        if (id == 0) {
            storageAPI.postMessage(getRandomMessage());
        } else if (id == 1) {
            storageAPI.allMessages();
        } else if (id == 2) {
            storageAPI.searchMessage(getRandomWord());
        } else if (id == 3) {
            storageAPI.messageDetails((long) (random.nextInt(currentRequests) % currentRequests));
        } else if (id == 4) {
            storageAPI.searchAndDetails(getRandomWord());
        } else if (id == 5) {
            storageAPI.deleteAllMessages();
        } else {
            throw new RuntimeException("The universe is broken");
        }

        currentRequests += 1;
    }

    String getRandomWord() {
        StringBuilder stringBuilder = new StringBuilder(charsLimit);

        for (int i=0; i<charsLimit; i++) {
            char letter = (char)('a' + random.nextInt(26));
            stringBuilder.append(letter);
        }

        return stringBuilder.toString();
    }

    Message getRandomMessage() {
        StringBuilder messageBuilder = new StringBuilder();

        for (int i=0; i<wordsLimit; i++) {
            messageBuilder.append(getRandomWord()).append(" ");
        }

        String sender = getRandomWord();
        return new Message(sender, messageBuilder.toString());
    }

    abstract void nextRequest(StorageAPI storageAPI);
}
