import java.util.Random;

public abstract class LoadFaker {
    Random random = new Random(22335577);
    private final int charsLimit;
    private final int wordsLimit;
    private int currentRequests = 1;

    public LoadFaker(int charsLimit, int wordsLimit) {
        this.charsLimit = charsLimit;
        this.wordsLimit = wordsLimit;
    }

    BaseRequest getRequestFromId(int id) {
        try {
            if (id == StupidStreamObject.ObjectType.POST_MESSAGE.ordinal()) {
                return new RequestPostMessage(getRandomMessage());
            } else if (id == StupidStreamObject.ObjectType.GET_ALL_MESSAGES.ordinal()) {
                return new RequestAllMessages();
            } else if (id == StupidStreamObject.ObjectType.SEARCH_MESSAGES.ordinal()) {
                return new RequestSearchMessage(getRandomWord(charsLimit - 1));
            } else if (id == StupidStreamObject.ObjectType.GET_MESSAGE_DETAILS.ordinal()) {
                long messageId = random.nextInt(currentRequests) % currentRequests;
                return new RequestMessageDetails(messageId);
            } else if (id == StupidStreamObject.ObjectType.SEARCH_AND_DETAILS.ordinal()) {
                return new RequestSearchAndDetails(getRandomWord(charsLimit - 1));
            } else if (id == StupidStreamObject.ObjectType.DELETE_ALL_MESSAGES.ordinal()) {
                return new RequestDeleteAllMessages();
            } else {
                throw new RuntimeException("The universe is broken");
            }
        } finally {
            currentRequests += 1; // Need to do that after all.. a bit of a hack but hey
        }
    }

    void callFromId(int id, StorageAPI storageAPI) {
        storageAPI.handleRequest(getRequestFromId(id), Object.class); // Ignores the output anyways
    }

    void callFromObjectType(StupidStreamObject.ObjectType objectType, StorageAPI storageAPI) {
        callFromId(objectType.ordinal(), storageAPI);
    }

    String getRandomWord(int length) {
        StringBuilder stringBuilder = new StringBuilder(charsLimit);

        for (int i=0; i<length; i++) {
            char letter = (char)('a' + random.nextInt(26));
            stringBuilder.append(letter);
        }

        return stringBuilder.toString();
    }

    Message getRandomMessage() {
        StringBuilder messageBuilder = new StringBuilder();

        for (int i=0; i<wordsLimit; i++) {
            messageBuilder.append(getRandomWord(charsLimit - 1)).append(" ");
        }

        String sender = getRandomWord(charsLimit * wordsLimit);
        return new Message(sender, messageBuilder.toString());
    }

    abstract void nextRequest(StorageAPI storageAPI);
}
