import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FullSystemTest {
    private static final Logger LOGGER = Logger.getLogger(FullSystemTest.class.getName());
    private static final long timestamp = System.nanoTime();

    @BeforeClass
    public static void init() throws IOException, InterruptedException, ExecutionException {
        System.out.println("Initializing....");
        TestUtils.manualConsumerInitialization();
    }

    @Before
    public void refresh() throws IOException, InterruptedException, ExecutionException {
        System.out.println("Refreshing stuff...");
        TestUtils.manualConsumerInitialization().polyglotAPI.clearConfirmationListeners();
        TestUtils.manualConsumerInitialization().moveAllToLatest();
        TestUtils.deleteAllMessages();
    }

    @Test
    public void searchNoOccurrences() throws Exception {
        TestUtils.postMessage(new Message("Simeon", "Bobi",  "Hey", timestamp));
        TestUtils.postMessage(new Message("Simeon", "Bobi",  "What's up", timestamp));

        ResponseSearchMessage responseSearchMessage = TestUtils.searchMessage("non-existent");
        LOGGER.info("Tester: Response from search is " + responseSearchMessage);
        assertEquals(0, responseSearchMessage.getOccurrences().size());
    }

    @Test
    public void searchHasOccurrences() throws Exception {
        TestUtils.postMessage(new Message("Simeon", "Bobi",  "Hey", timestamp));
        TestUtils.postMessage(new Message("Simeon", "Bobi",  "What's up", timestamp));
        TestUtils.postMessage(new Message("Simeon", "Bobi",  "Hey", timestamp));
        TestUtils.postMessage(new Message("Simeon", "Bobi",  "Hey", timestamp));

        ResponseSearchMessage responseSearchMessage = TestUtils.searchMessage("Hey");
        assertEquals(3, responseSearchMessage.getOccurrences().size());
    }

    @Test
    public void searchAndDetailsSeparately() throws Exception {
        Message toSend = new Message("Simeon", "Bobi",  "Hey", timestamp);
        int cnt = 10;

        for (int i = 0; i < cnt; i++) {
            TestUtils.postMessage(toSend);

            TestUtils.postMessage(new Message("Someone else", "Someone else 2",
                "jibberish", timestamp));
            TestUtils.postMessage(new Message("Someone else", "Someone else 2",
                "jibberish", timestamp));
            TestUtils.postMessage(new Message("Someone else", "Someone else 2",
                "jibberish", timestamp));
        }


        ResponseSearchMessage responseSearchMessage = TestUtils.searchMessage("Hey");
        assertEquals(cnt, responseSearchMessage.getOccurrences().size());

        List<Long> messagesIds = responseSearchMessage.getOccurrences();

        for (long id : messagesIds) {
            ResponseMessageDetails details = TestUtils.messageDetails(id);
            assertEquals(toSend, details.getMessage());
        }

    }

    @Test
    public void allMessages() throws Exception {
        List<Message> toSend = new ArrayList<>();
        toSend.add(new Message("Simeon", "Bobi",  "Hey", timestamp));
        toSend.add(new Message("Simeon", "Bobi",  "What's up", timestamp));
        toSend.add(new Message("Simeon", "Bobi",  "It's a bit lonely", timestamp));
        toSend.add(new Message("Simeon", "Bobi",  "But hey", timestamp));
        toSend.add(new Message("Simeon", "Bobi",  "It's Christmas", timestamp));

        int additional = 10;
        for (int i = 0; i < additional; i++) {
            toSend.add(new Message("Simeon", "Bobi",  "Ho", timestamp));
        }

        for (Message mes : toSend) {
            TestUtils.postMessage(mes);
        }

        ResponseAllMessages responseAllMessages = TestUtils.getAllConvoMessages("Simeon", "Bobi");
        System.out.println("Received " + responseAllMessages);
        assertEquals(toSend.size(), responseAllMessages.getMessages().size());

        List<Message> messages = responseAllMessages.getMessages();
        assertTrue(toSend.containsAll(messages) && messages.containsAll(toSend));
    }

    @Test
    public void searchAndDetailsNoOccurrences() throws Exception {
        TestUtils.postMessage(new Message("Simeon", "Bobi",  "Hey", timestamp));
        TestUtils.postMessage(new Message("Simeon", "Bobi",  "What's up", timestamp));

        ResponseMessageDetails responseMessageDetails = TestUtils.searchAndDetails("non-existent");

        assertNull(responseMessageDetails.getMessage());
    }

    @Test
    public void searchAndDetails1Occurrence() throws Exception {
        Message simeonHey = new Message("Simeon", "Bobi",  "Hey", timestamp);

        TestUtils.postMessage(simeonHey);
        TestUtils.postMessage(new Message("Simeon", "Bobi",  "What's up", timestamp));

        ResponseMessageDetails responseMessageDetails = TestUtils.searchAndDetails("Hey");

        assertEquals(simeonHey, responseMessageDetails.getMessage());
    }

    @Test
    public void searchAndDetailsSnapshotIsolated() throws Exception {
        TestUtils.ManualTrinity manualTrinity = TestUtils.manualConsumerInitialization();
        PolyglotAPI polyglotAPI = manualTrinity.polyglotAPI;
        // Consume the NOP operation and make sure that they can communicate through Kafka
//            assertEquals(1, manualTrinity.progressLucene());
//            assertEquals(1, manualTrinity.progressPsql());

        Message simeonHey = new Message("Simeon", "Bobi", "Hey", timestamp);

        polyglotAPI.handleRequest(new RequestPostMessage(new Addressable(polyglotAPI.getResponseAddress()),
            simeonHey));
        polyglotAPI.handleRequest(new RequestPostMessage(new Addressable(polyglotAPI.getResponseAddress()),
            new Message("Simeon", "Bobi", "What's up", timestamp)));

        // Post the messages
        assertEquals(2, manualTrinity.progressLucene());
        assertEquals(2, manualTrinity.progressPsql());

        // Search & details request
        Future<ResponseMessageDetails> responseMessageDetailsFuture = polyglotAPI.handleRequest(
            new RequestSearchAndGetDetails(new Addressable(polyglotAPI.getResponseAddress()),
                simeonHey.getMessageText()), ResponseMessageDetails.class);

        // Progress just PSQL, which should take a snapshot of the data, and "wait" for Lucene
        assertEquals(1, manualTrinity.progressPsql());

        // Now delete all messages in PSQL
        polyglotAPI.handleRequest(new RequestDeleteConversation(new Addressable(polyglotAPI.getResponseAddress()),
            ConstantsMAPP.DEFAULT_USER, ConstantsMAPP.DEFAULT_USER));
        assertEquals(1, manualTrinity.progressPsql());

        // Make sure they are deleted in PSQL
        Future<ResponseAllMessages> allMessagesFuture =
            polyglotAPI.handleRequest(new RequestConvoMessages(new Addressable(polyglotAPI.getResponseAddress()),
                    ConstantsMAPP.DEFAULT_USER, ConstantsMAPP.DEFAULT_USER),
                ResponseAllMessages.class);

        assertEquals(1, manualTrinity.progressPsql());
        assertEquals(0, allMessagesFuture.get().getMessages().size());

        // Now allow Lucene to progress and contact PSQL
        assertEquals(3, manualTrinity.progressLucene());

        // Check that the result which we got back from PSQL is legit
        assertEquals(simeonHey, responseMessageDetailsFuture.get().getMessage());
    }

    @Test
    public void aLotOfSDs() throws Exception {
        int messagesToPost = 300;
        int messagesToSearch = 300;

        // Post messages
        for (int i = 1; i <= messagesToPost; i++) {
            TestUtils.postMessage(new Message("Simeon", "Bobi",  "Message " + i, timestamp));
        }

        Thread.sleep(5000);

        // Then search for them
        List<ResponseMessageDetails> details = new ArrayList<>();

        for (int i = 0; i < messagesToSearch; i++) {
            int toSearchFor = i % messagesToPost + 1;
            ResponseMessageDetails curDetails = TestUtils.searchAndDetails("Message " + toSearchFor);
            System.out.println("Details for " + i + " are " + curDetails);
            details.add(curDetails);
        }

        for (int i = 0; i < details.size(); i++) {
            String targetText = "Message " + (i % messagesToPost + 1);
            System.out.println("Checking message " + i + " for target text " + targetText);
            Message actualMessage = details.get(i).getMessage();
            assertEquals(targetText, actualMessage.getMessageText());
        }
    }

    @Test
    public void confirmationListenersWork() throws Exception {
        TestUtils.ManualTrinity manualTrinity = TestUtils.manualConsumerInitialization();
        PolyglotAPI polyglotAPI = manualTrinity.polyglotAPI;

        manualTrinity.progressAll();
        Thread.sleep(1000);
        manualTrinity.polyglotAPI.waitForAllConfirmations();

        int numRequests = 1;
        ArrayBlockingQueue<ConfirmationResponse> confirmationResponses = new ArrayBlockingQueue<>(numRequests);
        manualTrinity.polyglotAPI.registerConfirmationListener(confirmationResponse -> {
            try {
                System.out.println("Received confirmation!");
                confirmationResponses.put(confirmationResponse);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        for (int i = 0; i < numRequests; i++) {
            manualTrinity.polyglotAPI.handleRequest(
                new RequestPostMessage(new Addressable(polyglotAPI.getResponseAddress()),
                    new Message("Simeon", "Bobi",  "J Cole is the best", timestamp)));
        }

        manualTrinity.progressPsql();
        Thread.sleep(1000);
        manualTrinity.polyglotAPI.waitForAllConfirmations();

        assertEquals(numRequests, confirmationResponses.size());
        while (confirmationResponses.size() > 0) {
            ConfirmationResponse current = confirmationResponses.take();

            assertTrue(current.getFromStorageSystem().startsWith("psql"));
            assertEquals(RequestPostMessage.class.getName(), current.getObjectType());
        }
        System.out.println("Done checking PSQL's confirmations");

        manualTrinity.progressLucene();
        Thread.sleep(1000);
        manualTrinity.polyglotAPI.waitForAllConfirmations();

        assertEquals(numRequests, confirmationResponses.size());
        while (confirmationResponses.size() > 0) {
            ConfirmationResponse current = confirmationResponses.take();

            assertTrue(current.getFromStorageSystem().startsWith("lucene"));
            assertEquals(RequestPostMessage.class.getName(), current.getObjectType());
        }
    }

    @Test
    public void messageCountsIncreases() throws Exception {
        String sender = "simeon";
        String targetRecipient = "gosho";
        int unreadsExpected = 100;

        for (int i = 0; i < unreadsExpected; i++) {
            TestUtils.postMessage(new Message(sender, targetRecipient,  "hey m8", timestamp));
        }

        int unreads = TestUtils.getAllMessages(sender, targetRecipient);
        assertEquals(unreadsExpected, unreads);
    }

    @Test
    public void messageCountsDecreases() throws Exception {
        String sender = "Simeon";
        String targetRecipient = "gosho";
        String otherRecipient = "other";

        int unreadsExpected = 100;

        for (int i = 0; i < unreadsExpected; i++) {
            TestUtils.postMessage(new Message(sender, targetRecipient,  "hey m8", timestamp));
            TestUtils.postMessage(new Message(sender, otherRecipient,  "hey m8", timestamp));
        }

        TestUtils.deleteConversation(sender, targetRecipient); // This will trigger the count to be resetted

        int unreadsTarget = TestUtils.getAllMessages(sender, targetRecipient);
        int unreadsOther = TestUtils.getAllMessages(sender, otherRecipient);

        assertEquals(0, unreadsTarget);
        assertEquals(unreadsExpected, unreadsOther);
    }

    @Test
    public void deleteConversationPsql() throws ExecutionException, InterruptedException {
        String mainUser = "simeon";
        String friend1 = "gosho";
        String friend2 = "bobi";

        Message message1 = new Message(mainUser, friend1, "Hey bro", timestamp);
        Message message2 = new Message(mainUser, friend2, "Whats up", timestamp);

        TestUtils.postMessage(message1);
        TestUtils.postMessage(message2);

        // Delete just one of those conversations
        TestUtils.deleteConversation(mainUser, friend1);

        // Assert it is deleted
        ResponseAllMessages responseAllMessages = TestUtils.getAllConvoMessages(mainUser, friend1);
        assertEquals(List.of(), responseAllMessages.getMessages());

        // Assert that the other one is not deleted
        ResponseAllMessages responseAllMessages1 = TestUtils.getAllConvoMessages(mainUser, friend2);
        assertEquals(List.of(message2), responseAllMessages1.getMessages());

        // Delete everything
        TestUtils.deleteAllMessages();
        // Assert that it is deleted everywhere
        ResponseAllMessages responseAllMessages2 = TestUtils.getAllConvoMessages(mainUser, friend2);
        assertEquals(List.of(), responseAllMessages2.getMessages());
    }

    @Test
    public void deleteConversationLucene() throws ExecutionException, InterruptedException {
        String mainUser = "simeon";
        String friend1 = "gosho";
        String friend2 = "bobi";

        Message message1 = new Message(mainUser, friend1, "heybro", timestamp);
        Message message2 = new Message(mainUser, friend2, "whatsup", timestamp);

        ResponseSearchMessage responseSearchMessage1, responseSearchMessage2;

        // Post and assert it is there
        TestUtils.postMessage(message1);
        TestUtils.postMessage(message2);
        System.out.println("Posted messages, searching now");

        responseSearchMessage1 = TestUtils.searchMessage(message1.getMessageText());
        responseSearchMessage2 = TestUtils.searchMessage(message2.getMessageText());
        assertEquals(1, responseSearchMessage1.getOccurrences().size());
        assertEquals(1, responseSearchMessage2.getOccurrences().size());

        // Delete just one of those conversations and test
        TestUtils.deleteConversation(mainUser, friend1);

        responseSearchMessage1 = TestUtils.searchMessage(message1.getMessageText());
        responseSearchMessage2 = TestUtils.searchMessage(message2.getMessageText());
        assertEquals(0, responseSearchMessage1.getOccurrences().size());
        assertEquals(1, responseSearchMessage2.getOccurrences().size());

        // Delete everything and test that it is deleted everywhere
        TestUtils.deleteAllMessages();
        responseSearchMessage1 = TestUtils.searchMessage(message1.getMessageText());
        responseSearchMessage2 = TestUtils.searchMessage(message2.getMessageText());
        assertEquals(0, responseSearchMessage1.getOccurrences().size());
        assertEquals(0, responseSearchMessage2.getOccurrences().size());
    }

    @Test
    public void playin() throws ExecutionException, InterruptedException {
        Message message1 = new Message("simeon", "bobi", "heyyo", 1);
        Message message2 = new Message("bobi", "simeon", "heyyo", 1);

        TestUtils.postMessage(message1);
        for (int i=1; i<=20; i++) {
            TestUtils.sleep1(false);
        }

        long start = System.nanoTime();
        System.out.println("Done sending sleeps!");
        TestUtils.postMessage(message2);
        ResponseAllMessages responseAllMessages = TestUtils.getAllConvoMessages("simeon", "bobi");
        long elapsed = System.nanoTime() - start;

        System.out.println("Elapsed " + elapsed / 1000000L + " ms");

        assertEquals(List.of(message1, message2), responseAllMessages.getMessages());
    }
}
