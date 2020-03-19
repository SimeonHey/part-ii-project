import java.util.Random;

public abstract class LoadFaker {
    Random random = new Random(22335577);
    private final int charsLimit = 3;
    private final int wordsLimit = 20;
    int currentRequests = 0;

    void callFromId(int id, StorageAPI storageAPI) {
        if (id == StupidStreamObject.ObjectType.POST_MESSAGE.ordinal()) {
            storageAPI.postMessage(getRandomMessage());
        } else if (id == StupidStreamObject.ObjectType.GET_ALL_MESSAGES.ordinal()) {
            storageAPI.allMessages();
        } else if (id == StupidStreamObject.ObjectType.SEARCH_MESSAGES.ordinal()) {
            storageAPI.searchMessage(getRandomWord());
        } else if (id == StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS.ordinal()) {
            storageAPI.messageDetails((long) (random.nextInt(currentRequests) % currentRequests));
        } else if (id == StupidStreamObject.ObjectType.SEARCH_AND_DETAILS.ordinal()) {
            storageAPI.searchAndDetails(getRandomWord());
        } else if (id == StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES.ordinal()) {
            storageAPI.deleteAllMessages();
        } else {
            throw new RuntimeException("The universe is broken");
        }

        currentRequests += 1;
    }

    void callFromObjectType(StupidStreamObject.ObjectType objectType, StorageAPI storageAPI) {
        callFromId(objectType.ordinal(), storageAPI);
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
