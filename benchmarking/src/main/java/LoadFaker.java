import java.util.Random;

public abstract class LoadFaker {
    public enum Events {
        POST_MESSAGE(RequestPostMessage.class),
        GET_ALL_MESSAGES(RequestAllMessages.class),
        GET_MESSAGE_DETAILS(RequestMessageDetails.class),
        SEARCH_MESSAGES(RequestSearchMessage.class),
        SEARCH_AND_DETAILS(RequestSearchAndDetails.class),
        SLEEP_1(RequestSleep1.class),
        DELETE_ALL_MESSAGES(RequestDeleteAllMessages.class);
        
        private final Class<? extends BaseEvent> theClass;

        Events(Class<? extends BaseEvent> theClass) {
            this.theClass = theClass;
        }
    }

    private int findEventByName(String eventName) {
        for (Events value : Events.values()) {
            if (value.theClass.getName().equals(eventName)) {
                return value.ordinal();
            }
        }

        throw new RuntimeException("Can't find the event");
    }

    Random random = new Random(22335577);
    private final int charsLimit;
    private final int wordsLimit;
    private int currentRequests = 1;

    public LoadFaker(int charsLimit, int wordsLimit) {
        this.charsLimit = charsLimit;
        this.wordsLimit = wordsLimit;
    }

    BaseEvent getRequestFromId(int id, Addressable responseAddress) {
        try {
            if (id == Events.POST_MESSAGE.ordinal()) {
                return new RequestPostMessage(responseAddress,
                    getRandomMessage(), getRandomWord(charsLimit * wordsLimit));
            } else if (id == Events.GET_ALL_MESSAGES.ordinal()) {
                return new RequestAllMessages(responseAddress, ConstantsMAPP.DEFAULT_USER);
            } else if (id == Events.SEARCH_MESSAGES.ordinal()) {
                return new RequestSearchMessage(responseAddress, getRandomWord(charsLimit - 1));
            } else if (id == Events.GET_MESSAGE_DETAILS.ordinal()) {
                long messageId = random.nextInt(currentRequests) % currentRequests;
                return new RequestMessageDetails(responseAddress, messageId);
            } else if (id == Events.SEARCH_AND_DETAILS.ordinal()) {
                return new RequestSearchAndDetails(responseAddress, getRandomWord(charsLimit - 1));
            } else if (id == Events.DELETE_ALL_MESSAGES.ordinal()) {
                return new RequestDeleteAllMessages(responseAddress);
            } else if (id == Events.SLEEP_1.ordinal()) {
                return new RequestSleep1(responseAddress);
            } else {
                throw new RuntimeException("The universe is broken");
            }
        } finally {
            currentRequests += 1; // Need to do that after all.. a bit of a hack but hey
        }
    }

    void callFromId(int id, StorageAPI storageAPI) {
        storageAPI.handleRequest(getRequestFromId(id, new Addressable(storageAPI.getResponseAddress())), Object.class); // Ignores the
        // output
        // anyways
    }

    void callFromObjectType(String objectType, StorageAPI storageAPI) {
        callFromId(findEventByName(objectType), storageAPI);
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
