import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FullSystemTest {
    private static final Logger LOGGER = Logger.getLogger(FullSystemTest.class.getName());
    
    @Test
    public void searchNoOccurrences() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Utils.letThatSinkIn(storageAPI, () -> {
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "Hey"), ConstantsMAPP.UNKNOWN_RECIPIENT));
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "What's up"), ConstantsMAPP.UNKNOWN_RECIPIENT));
            });

            ResponseSearchMessage responseSearchMessage =
                storageAPI.handleRequest(new RequestSearchMessage(new Addressable(storageAPI.getResponseAddress()),
                        "non-existent"),
                    ResponseSearchMessage.class).get();
            LOGGER.info("Tester: Response from search is " + responseSearchMessage);
            assertEquals(0, responseSearchMessage.getOccurrences().size());
        }
    }

    @Test
    public void searchHasOccurrences() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Utils.letThatSinkIn(storageAPI, () -> {
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "Hey"), ConstantsMAPP.UNKNOWN_RECIPIENT));
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "What's up"), ConstantsMAPP.UNKNOWN_RECIPIENT));
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "Hey"), ConstantsMAPP.UNKNOWN_RECIPIENT));
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "Hey"), ConstantsMAPP.UNKNOWN_RECIPIENT));
            });

            Thread.sleep(1000);

            ResponseSearchMessage responseSearchMessage =
                storageAPI.handleRequest(new RequestSearchMessage(new Addressable(storageAPI.getResponseAddress()),
                        "Hey"),
                    ResponseSearchMessage.class).get();
            assertEquals(3, responseSearchMessage.getOccurrences().size());
        }
    }

    @Test
    public void searchAndDetailsSeparately() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Message toSend = new Message("Simeon", "Hey");
            int cnt = 10;

            Utils.letThatSinkIn(storageAPI, () -> {
                for (int i = 0; i < cnt; i++) {
                    storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                        toSend, ConstantsMAPP.UNKNOWN_RECIPIENT));

                    storageAPI.handleRequest(
                        new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                            new Message("Someone else", "jibberish"), ConstantsMAPP.UNKNOWN_RECIPIENT));
                    storageAPI.handleRequest(
                        new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                            new Message("Someone else", "jibberish"), ConstantsMAPP.UNKNOWN_RECIPIENT));
                    storageAPI.handleRequest(
                        new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                            new Message("Someone else", "jibberish"), ConstantsMAPP.UNKNOWN_RECIPIENT));
                }
            });

            Thread.sleep(5000);

            ResponseSearchMessage responseSearchMessage =
                storageAPI.handleRequest(new RequestSearchMessage(new Addressable(storageAPI.getResponseAddress()),
                        "Hey"),
                    ResponseSearchMessage.class).get();
            assertEquals(cnt, responseSearchMessage.getOccurrences().size());

            List<Long> messagesIds = responseSearchMessage.getOccurrences();

            for (long id : messagesIds) {
                ResponseMessageDetails details =
                    storageAPI.handleRequest(
                        new RequestMessageDetails(new Addressable(storageAPI.getResponseAddress()), id),
                        ResponseMessageDetails.class).get();
                assertEquals(toSend, details.getMessage());
            }
        }
    }

    @Test
    public void allMessages() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            List<Message> toSend = new ArrayList<>();
            toSend.add(new Message("Simeon", "Hey"));
            toSend.add(new Message("Simeon", "What's up"));
            toSend.add(new Message("Simeon", "It's a bit lonely"));
            toSend.add(new Message("Simeon", "But hey"));
            toSend.add(new Message("Simeon", "It's Christmas"));

            int additional = 10;
            for (int i = 0; i < additional; i++) {
                toSend.add(new Message("Simeon", "Ho"));
            }

            Utils.letThatSinkIn(storageAPI, () -> {
                for (Message mes : toSend) {
                    storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                        mes, ConstantsMAPP.UNKNOWN_RECIPIENT));
                }
            });

            Thread.sleep(5000);

            ResponseAllMessages responseAllMessages =
                storageAPI.handleRequest(new RequestAllMessages(new Addressable(storageAPI.getResponseAddress())),
                    ResponseAllMessages.class).get();
            System.out.println("Received " + responseAllMessages);
            assertEquals(toSend.size(), responseAllMessages.getMessages().size());

            List<Message> messages = responseAllMessages.getMessages();
            assertTrue(toSend.containsAll(messages) && messages.containsAll(toSend));
        }
    }


    @Test
    public void searchAndDetailsNoOccurrences() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Utils.letThatSinkIn(storageAPI, () -> {
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "Hey"), ConstantsMAPP.UNKNOWN_RECIPIENT));
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "What's up"), ConstantsMAPP.UNKNOWN_RECIPIENT));
            });

            ResponseMessageDetails responseMessageDetails = storageAPI.handleRequest(
                new RequestSearchAndDetails(new Addressable(storageAPI.getResponseAddress()), "non-existent"),
                ResponseMessageDetails.class).get();

            assertNull(responseMessageDetails.getMessage());
        }
    }

    @Test
    public void searchAndDetails1Occurrence() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Message simeonHey = new Message("Simeon", "Hey");

            Utils.letThatSinkIn(storageAPI, () -> {
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    simeonHey, ConstantsMAPP.UNKNOWN_RECIPIENT));
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "What's up"), ConstantsMAPP.UNKNOWN_RECIPIENT));
            });

            ResponseMessageDetails responseMessageDetails = storageAPI.handleRequest(
                new RequestSearchAndDetails(new Addressable(storageAPI.getResponseAddress()), "Hey"),
                ResponseMessageDetails.class).get();

            assertEquals(simeonHey, responseMessageDetails.getMessage());
        }
    }

    @Test
    public void searchAndDetailsSnapshotIsolated() throws Exception {
        try (Utils.ManualTrinity manualTrinity = Utils.manualConsumerInitialization()) {
            StorageAPI storageAPI = manualTrinity.storageAPI;
            // Consume the NOP operation and make sure that they can communicate through Kafka
            assertEquals(1, manualTrinity.progressLucene());
            assertEquals(1, manualTrinity.progressPsql());

            Message simeonHey = new Message("Simeon", "Hey");

            storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                simeonHey, ConstantsMAPP.UNKNOWN_RECIPIENT));
            storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "What's up"), ConstantsMAPP.UNKNOWN_RECIPIENT));

            // Post the messages
            assertEquals(2, manualTrinity.progressLucene());
            assertEquals(2, manualTrinity.progressPsql());

            // Search & details request
            Future<ResponseMessageDetails> responseMessageDetailsFuture = storageAPI.handleRequest(
                new RequestSearchAndDetails(new Addressable(storageAPI.getResponseAddress()),
                    simeonHey.getMessageText()), ResponseMessageDetails.class);

            // Progress just PSQL, which should take a snapshot of the data, and "wait" for Lucene
            assertEquals(1, manualTrinity.progressPsql());

            // Now delete all messages in PSQL
            storageAPI.handleRequest(new RequestDeleteAllMessages(new Addressable(storageAPI.getResponseAddress())));
            assertEquals(1, manualTrinity.progressPsql());

            // Make sure they are deleted in PSQL
            Future<ResponseAllMessages> allMessagesFuture =
                storageAPI.handleRequest(new RequestAllMessages(new Addressable(storageAPI.getResponseAddress())),
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
        try (Utils.ManualTrinity manualTrinity = Utils.manualConsumerInitialization()) {
            StorageAPI storageAPI = manualTrinity.storageAPI;

            int messagesToPost = 300;
            int messagesToSearch = 300;

            // Post messages
            for (int i=1; i<=messagesToPost; i++) {
                manualTrinity.storageAPI.handleRequest(
                    new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("simeon", "Message " + i), ConstantsMAPP.UNKNOWN_RECIPIENT));
            }

            // Then search for them
            List<Future<ResponseMessageDetails>> futureDetails = new ArrayList<>();

            for (int i=0; i<messagesToSearch; i++) {
                int toSearchFor = i % messagesToPost + 1;
                Future<ResponseMessageDetails> detailsFuture = manualTrinity.storageAPI.handleRequest(
                    new RequestSearchAndDetails(new Addressable(storageAPI.getResponseAddress()),
                        "Message " + toSearchFor), ResponseMessageDetails.class);

                futureDetails.add(detailsFuture);
            }

            manualTrinity.progressLucene();
            manualTrinity.progressPsql();

            for (int i=0; i<futureDetails.size(); i++) {
                String targetText = "Message " + (i % messagesToPost + 1);
                Message actualMessage =  futureDetails.get(i).get().getMessage();

                assertEquals(targetText, actualMessage.getMessageText());
            }
        }
    }

    @Test
    public void confirmationListenersWork() throws Exception {
        try (Utils.ManualTrinity manualTrinity = Utils.manualConsumerInitialization()) {
            StorageAPI storageAPI = manualTrinity.storageAPI;

            int numRequests = 100;
            ArrayBlockingQueue<ConfirmationResponse> confirmationResponses =
                new ArrayBlockingQueue<>(numRequests);
            manualTrinity.storageAPI.registerConfirmationListener(confirmationResponse -> {
                try {
                    confirmationResponses.put(confirmationResponse);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            for (int i=0; i<100; i++) {
                manualTrinity.storageAPI.handleRequest(
                    new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("Simeon", "J Cole is the best"), ConstantsMAPP.UNKNOWN_RECIPIENT));
            }

            manualTrinity.progressPsql();

            Thread.sleep(1000);
            assertEquals(numRequests, confirmationResponses.size());
            while (confirmationResponses.size() > 0) {
                ConfirmationResponse current = confirmationResponses.take();

                assertTrue(current.getFromStorageSystem().startsWith("psql"));
                assertEquals(RequestPostMessage.class.getName(), current.getObjectType());
            }

            manualTrinity.progressLucene();

            Thread.sleep(1000);
            assertEquals(numRequests, confirmationResponses.size());
            while (confirmationResponses.size() > 0) {
                ConfirmationResponse current = confirmationResponses.take();

                assertTrue(current.getFromStorageSystem().startsWith("lucene"));
                assertEquals(RequestPostMessage.class.getName(), current.getObjectType());
            }
        }
    }

    @Test
    public void messageCountsIncreases() throws Exception {
        try (Utils.ManualTrinity manualTrinity = Utils.manualConsumerInitialization()) {
            StorageAPI storageAPI = manualTrinity.storageAPI;
            String targetRecipient = "gosho";

            int unreadsExpected = 100;

            for (int i = 0; i < unreadsExpected; i++) {
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("simeon", "hey m8"), targetRecipient));
            }
            manualTrinity.progressVavr();
            storageAPI.waitForAllConfirmations();

            CompletableFuture<Integer> unreadMessagesFuture = storageAPI
                .handleRequest(new RequestGetUnreadMessages(new Addressable(storageAPI.getResponseAddress()),
                    targetRecipient), Integer.class);
            manualTrinity.progressVavr();
            storageAPI.waitForAllConfirmations();

            int unreads = unreadMessagesFuture.get();
            assertEquals(unreadsExpected, unreads);
        }
    }

    @Test
    public void messageCountsDecreases() throws Exception {
        try (Utils.ManualTrinity manualTrinity = Utils.manualConsumerInitialization()) {
            StorageAPI storageAPI = manualTrinity.storageAPI;
            String targetRecipient = "gosho";

            int unreadsExpected = 100;

            for (int i=0; i<unreadsExpected; i++) {
                storageAPI.handleRequest(new RequestPostMessage(new Addressable(storageAPI.getResponseAddress()),
                    new Message("simeon", "hey m8"), targetRecipient));
            }
            manualTrinity.progressVavr();
            storageAPI.waitForAllConfirmations();

            storageAPI.handleRequest(new RequestAllMessages(new Addressable(storageAPI.getResponseAddress())));
            storageAPI.waitForAllConfirmations();

            CompletableFuture<Integer> unreadMessagesFuture = storageAPI
                .handleRequest(new RequestGetUnreadMessages(new Addressable(storageAPI.getResponseAddress()),
                    targetRecipient), Integer.class);
            manualTrinity.progressVavr();
            storageAPI.waitForAllConfirmations();

            int unreads = unreadMessagesFuture.get();
            assertEquals(0, unreads);
        }
    }
}