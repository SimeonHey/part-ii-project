import java.util.Random;

public abstract class LoadFaker {
    public enum Events {
        POST_MESSAGE(RequestPostMessage.class),
        GET_ALL_THREAD_MESSAGES(RequestConvoMessages.class),
        GET_MESSAGE_DETAILS(RequestMessageDetails.class),
        SEARCH_MESSAGES(RequestSearchMessage.class),
        SEARCH_AND_DETAILS(RequestSearchAndGetDetails.class),
//        SLEEP_1(RequestSleep1.class),
//        DELETE_ALL_MESSAGE(RequestDeleteAllMessages.class),
        DELETE_CONVO_THREAD(RequestDeleteConversation.class),
        GET_UNREADS(RequestGetUnreadMessages.class);
        
        private final Class<? extends EventBase> theClass;

        Events(Class<? extends EventBase> theClass) {
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

    EventBase getRequestFromId(int id, Addressable responseAddress) {
        try {
            String user1 = getRandomWord(charsLimit-1);
            String user2 = getRandomWord(charsLimit-1);
            String word = getRandomWord(charsLimit - 1);
            Message message = getRandomMessage();

            if (id == Events.POST_MESSAGE.ordinal()) {
                return new RequestPostMessage(responseAddress, message);
            } else if (id == Events.GET_ALL_THREAD_MESSAGES.ordinal()) {
                return new RequestConvoMessages(responseAddress, user1, user2);
            } else if (id == Events.SEARCH_MESSAGES.ordinal()) {
                return new RequestSearchMessage(responseAddress, word);
            } else if (id == Events.GET_MESSAGE_DETAILS.ordinal()) {
                long messageId = random.nextInt(currentRequests) % currentRequests;
                return new RequestMessageDetails(responseAddress, messageId);
            } else if (id == Events.SEARCH_AND_DETAILS.ordinal()) {
                return new RequestSearchAndGetDetails(responseAddress, word);
            } else if (id == Events.DELETE_CONVO_THREAD.ordinal()) {
                return new RequestDeleteConversation(responseAddress, user1, user2);
            } /*else if (id == Events.SLEEP_1.ordinal()) {
                return new RequestSleep1(responseAddress);
            }*/
            else if (id == Events.GET_UNREADS.ordinal()) {
                return new RequestGetUnreadMessages(responseAddress, user1);
            } else {
                throw new RuntimeException("The universe is broken");
            }
        } finally {
            currentRequests += 1; // Need to do that after all.. a bit of a hack but hey
        }
    }

    void callFromId(int id, PolyglotAPI polyglotAPI) {
        polyglotAPI.handleRequest(getRequestFromId(id, new Addressable(polyglotAPI.getResponseAddress())), Object.class); // Ignores the
        // output
        // anyways
    }

    void callFromObjectType(String objectType, PolyglotAPI polyglotAPI) {
        callFromId(findEventByName(objectType), polyglotAPI);
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

        String sender = getRandomWord(charsLimit - 1);
        String recipient = getRandomWord(charsLimit - 1);
        return new Message(sender, recipient, messageBuilder.toString(), System.nanoTime());
    }

    abstract void nextRequest(PolyglotAPI polyglotAPI);
}
