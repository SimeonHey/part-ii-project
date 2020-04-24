import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FullSystemTest {
    private static final Logger LOGGER = Logger.getLogger(FullSystemTest.class.getName());
    private static final long timestamp = System.nanoTime();

    @BeforeClass
    public static void init() throws IOException {
        System.out.println("Initializing....");
        TestUtils.manualConsumerInitialization();
    }

    @Before
    public void refresh() throws IOException {
        System.out.println("Refreshing stuff...");
        TestUtils. manualConsumerInitialization().storageAPI.clearConfirmationListeners();
        TestUtils.manualConsumerInitialization().moveAllToLatest();
        TestUtils.deleteAllMessages();
    }

    @Test
    public void searchNoOccurrences() throws Exception {
        TestUtils.postMessage(new Message("Simeon", "Hey", timestamp));
        TestUtils.postMessage(new Message("Simeon", "What's up", timestamp));

        ResponseSearchMessage responseSearchMessage = TestUtils.searchMessage("non-existent");
        LOGGER.info("Tester: Response from search is " + responseSearchMessage);
        assertEquals(0, responseSearchMessage.getOccurrences().size());
    }

    @Test
    public void searchHasOccurrences() throws Exception {
        TestUtils.postMessage(new Message("Simeon", "Hey", timestamp));
        TestUtils.postMessage(new Message("Simeon", "What's up", timestamp));
        TestUtils.postMessage(new Message("Simeon", "Hey", timestamp));
        TestUtils.postMessage(new Message("Simeon", "Hey", timestamp));

        ResponseSearchMessage responseSearchMessage = TestUtils.searchMessage("Hey");
        assertEquals(3, responseSearchMessage.getOccurrences().size());
    }

    @Test
    public void searchAndDetailsSeparately() throws Exception {
        Message toSend = new Message("Simeon", "Hey", timestamp);
        int cnt = 10;

        for (int i = 0; i < cnt; i++) {
            TestUtils.postMessage(toSend);

            TestUtils.postMessage(new Message("Someone else", "jibberish", timestamp));
            TestUtils.postMessage(new Message("Someone else", "jibberish", timestamp));
            TestUtils.postMessage(new Message("Someone else", "jibberish", timestamp));
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
        toSend.add(new Message("Simeon", "Hey", timestamp));
        toSend.add(new Message("Simeon", "What's up", timestamp));
        toSend.add(new Message("Simeon", "It's a bit lonely", timestamp));
        toSend.add(new Message("Simeon", "But hey", timestamp));
        toSend.add(new Message("Simeon", "It's Christmas", timestamp));

        int additional = 10;
        for (int i = 0; i < additional; i++) {
            toSend.add(new Message("Simeon", "Ho", timestamp));
        }


        for (Message mes : toSend) {
            TestUtils.postMessage(mes);
        }


        ResponseAllMessages responseAllMessages = TestUtils.allMessages();
        System.out.println("Received " + responseAllMessages);
        assertEquals(toSend.size(), responseAllMessages.getMessages().size());

        List<Message> messages = responseAllMessages.getMessages();
        assertTrue(toSend.containsAll(messages) && messages.containsAll(toSend));
    }

    @Test
    public void searchAndDetailsNoOccurrences() throws Exception {
        TestUtils.postMessage(new Message("Simeon", "Hey", timestamp));
        TestUtils.postMessage(new Message("Simeon", "What's up", timestamp));

        ResponseMessageDetails responseMessageDetails = TestUtils.searchAndDetails("non-existent");

        assertNull(responseMessageDetails.getMessage());
    }

    @Test
    public void searchAndDetails1Occurrence() throws Exception {
        Message simeonHey = new Message("Simeon", "Hey", timestamp);

        TestUtils.postMessage(simeonHey);
        TestUtils.postMessage(new Message("Simeon", "What's up", timestamp));

        ResponseMessageDetails responseMessageDetails = TestUtils.searchAndDetails("Hey");

        assertEquals(simeonHey, responseMessageDetails.getMessage());
    }

    @Test
    public void searchAndDetailsSnapshotIsolated() throws Exception {
        try (TestUtils.ManualTrinity manualTrinity = TestUtils.manualConsumerInitialization()) {
            StorageAPI storageAPI = manualTrinity.storageAPI;
            // Consume the NOP operation and make sure that they can communicate through Kafka
//            assertEquals(1, manualTrinity.progressLucene());
//            assertEquals(1, manualTrinity.progressPsql());

            Message simeonHey = new Message("Simeon", "Hey", timestamp);

            storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                simeonHey, ConstantsMAPP.DEFAULT_USER));
            storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                new Message("Simeon", "What's up", timestamp), ConstantsMAPP.DEFAULT_USER));

            // Post the messages
            assertEquals(2, manualTrinity.progressLucene());
            assertEquals(2, manualTrinity.progressPsql());

            // Search & details request
            Future<ResponseMessageDetails> responseMessageDetailsFuture = storageAPI.handleRequest(
                new RequestSearchAndGetDetails(new Addressable(storageAPI.getResponseAddress()),
                    simeonHey.getMessageText()), ResponseMessageDetails.class);

            // Progress just PSQL, which should take a snapshot of the data, and "wait" for Lucene
            assertEquals(1, manualTrinity.progressPsql());

            // Now delete all messages in PSQL
            storageAPI.handleRequest(new RequestDeleteAllMessages(new Addressable(storageAPI.getResponseAddress())));
            assertEquals(1, manualTrinity.progressPsql());

            // Make sure they are deleted in PSQL
            Future<ResponseAllMessages> allMessagesFuture =
                storageAPI.handleRequest(new RequestAllMessages(new Addressable(storageAPI.getResponseAddress()),
                        ConstantsMAPP.DEFAULT_USER),
                    ResponseAllMessages.class);

            assertEquals(1, manualTrinity.progressPsql());
            assertEquals(0, allMessagesFuture.get().getMessages().size());

            // Now allow Lucene to progress and contact PSQL
            assertEquals(3, manualTrinity.progressLucene());

            // Check that the result which we got back from PSQL is legit
            assertEquals(simeonHey, responseMessageDetailsFuture.get().getMessage());
        }
    }

    @Test
    public void aLotOfSDs() throws Exception {
        int messagesToPost = 300;
        int messagesToSearch = 300;

        // Post messages
        for (int i = 1; i <= messagesToPost; i++) {
            TestUtils.postMessage(new Message("simeon", "Message " + i, timestamp));
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
        StorageAPI storageAPI = manualTrinity.storageAPI;

        manualTrinity.progressAll();
        Thread.sleep(1000);
        manualTrinity.storageAPI.waitForAllConfirmations();

        int numRequests = 1;
        ArrayBlockingQueue<ConfirmationResponse> confirmationResponses = new ArrayBlockingQueue<>(numRequests);
        manualTrinity.storageAPI.registerConfirmationListener(confirmationResponse -> {
            try {
                System.out.println("Received confirmation!");
                confirmationResponses.put(confirmationResponse);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        for (int i = 0; i < numRequests; i++) {
            manualTrinity.storageAPI.handleRequest(
                new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "J Cole is the best", timestamp), ConstantsMAPP.DEFAULT_USER));
        }

        manualTrinity.progressPsql();
        Thread.sleep(1000);
        manualTrinity.storageAPI.waitForAllConfirmations();

        assertEquals(numRequests, confirmationResponses.size());
        while (confirmationResponses.size() > 0) {
            ConfirmationResponse current = confirmationResponses.take();

            assertTrue(current.getFromStorageSystem().startsWith("psql"));
            assertEquals(RequestPostMessage.class.getName(), current.getObjectType());
        }
        System.out.println("Done checking PSQL's confirmations");

        manualTrinity.progressLucene();
        Thread.sleep(1000);
        manualTrinity.storageAPI.waitForAllConfirmations();

        assertEquals(numRequests, confirmationResponses.size());
        while (confirmationResponses.size() > 0) {
            ConfirmationResponse current = confirmationResponses.take();

            assertTrue(current.getFromStorageSystem().startsWith("lucene"));
            assertEquals(RequestPostMessage.class.getName(), current.getObjectType());
        }
    }

    @Test
    public void messageCountsIncreases() throws Exception {
        String targetRecipient = "gosho";
        int unreadsExpected = 100;

        for (int i = 0; i < unreadsExpected; i++) {
            TestUtils.postMessage(new Message("simeon", "hey m8", timestamp), targetRecipient);
        }

        int unreads = TestUtils.getUnreads(targetRecipient);
        assertEquals(unreadsExpected, unreads);
    }

    @Test
    public void messageCountsDecreases() throws Exception {
        String targetRecipient = "gosho";
        String otherRecipient = "other";

        int unreadsExpected = 100;

        for (int i = 0; i < unreadsExpected; i++) {
            TestUtils.postMessage(new Message("simeon", "hey m8", timestamp), targetRecipient);
            TestUtils.postMessage(new Message("simeon", "hey m8", timestamp), otherRecipient);
        }

        TestUtils.allMessages(targetRecipient);

        int unreadsTarget = TestUtils.getUnreads(targetRecipient);
        int unreadsOther = TestUtils.getUnreads(otherRecipient);

        assertEquals(0, unreadsTarget);
        assertEquals(unreadsExpected, unreadsOther);
    }
}
