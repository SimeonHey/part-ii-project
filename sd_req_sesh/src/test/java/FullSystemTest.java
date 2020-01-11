import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class FullSystemTest {
    @Test
    public void searchNoOccurrences() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Utils.letThatSinkIn(() -> {
                storageAPI.postMessage(new Message("Simeon", "Hey"));
                storageAPI.postMessage(new Message("Simeon", "What's up"));
            });

            ResponseSearchMessage responseSearchMessage = storageAPI.searchMessage("non-existent");
            assertEquals(0, responseSearchMessage.getOccurrences().size());
        }
    }

    @Test
    public void searchHasOccurrences() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Utils.letThatSinkIn(() -> {
                storageAPI.postMessage(new Message("Simeon", "Hey"));
                storageAPI.postMessage(new Message("Simeon", "What's up"));
                storageAPI.postMessage(new Message("Simeon", "Hey"));
                storageAPI.postMessage(new Message("Simeon", "Hey"));
            });

            ResponseSearchMessage responseSearchMessage = storageAPI.searchMessage("Hey");
            assertEquals(3, responseSearchMessage.getOccurrences().size());
        }
    }

    @Test
    public void searchAndDetailsSeparately() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Message toSend = new Message("Simeon", "Hey");
            int cnt = 10;

            Utils.letThatSinkIn(() -> {
                for (int i = 0; i < cnt; i++) {
                    storageAPI.postMessage(toSend);

                    storageAPI.postMessage(new Message("Someone else", "jibberish"));
                    storageAPI.postMessage(new Message("Someone else", "jibberish"));
                    storageAPI.postMessage(new Message("Someone else", "jibberish"));
                }
            });

            ResponseSearchMessage responseSearchMessage = storageAPI.searchMessage("Hey");
            assertEquals(cnt, responseSearchMessage.getOccurrences().size());

            List<Long> messagesIds = responseSearchMessage.getOccurrences();

            for (long id : messagesIds) {
                ResponseMessageDetails details = storageAPI.messageDetails(id);
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

            Utils.letThatSinkIn(() -> {
                for (Message mes : toSend) {
                    storageAPI.postMessage(mes);
                }
            });

            ResponseAllMessages responseSearchMessage = storageAPI.allMessages();
            assertEquals(toSend.size(), responseSearchMessage.getMessages().size());

            List<Message> messages = responseSearchMessage.getMessages();
            assertEquals(toSend, messages);
        }
    }

    @Test
    public void searchAndDetailsNoOccurrences() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Utils.letThatSinkIn(() -> {
                storageAPI.postMessage(new Message("Simeon", "Hey"));
                storageAPI.postMessage(new Message("Simeon", "What's up"));
            });

            ResponseMessageDetails responseMessageDetails =
                storageAPI.searchAndDetails("non-existent");

            assertNull(responseMessageDetails.getMessage());
        }
    }

    @Test
    public void searchAndDetails1Occurrence() throws Exception {
        try (Utils.Trinity trinity = Utils.basicInitialization()) {
            StorageAPI storageAPI = trinity.storageAPI;

            Message simeonHey = new Message("Simeon", "Hey");

            Utils.letThatSinkIn(() -> {
                storageAPI.postMessage(simeonHey);
                storageAPI.postMessage(new Message("Simeon", "What's up"));
            });

            ResponseMessageDetails responseMessageDetails =
                storageAPI.searchAndDetails("Hey");

            assertEquals(simeonHey, responseMessageDetails.getMessage());
        }
    }

    @Test
    public void searchAndDetailsSnapshotIsolated() throws Exception {
        try (Utils.ManualTrinity manualTrinity = Utils.manualConsumerInitialization()) {
            ExecutorService executorService = Executors.newFixedThreadPool(2);

            StorageAPI storageAPI = manualTrinity.storageAPI;
            // Consume the NOP operation and make sure that they can communicate through Kafka
//            assertEquals(1, manualTrinity.progressLucene());
//            assertEquals(1, manualTrinity.progressPsql());

            Message simeonHey = new Message("Simeon", "Hey");

            storageAPI.postMessage(simeonHey);
            storageAPI.postMessage(new Message("Simeon", "What's up"));

            // Post the messages
            assertEquals(2, manualTrinity.progressLucene());
            assertEquals(2, manualTrinity.progressPsql());

            // Search & details request
            Future<ResponseMessageDetails> responseMessageDetailsFuture =
                executorService.submit(() -> storageAPI.searchAndDetails(simeonHey.getMessageText()));

            Thread.sleep(1000);

            // Progress just PSQL, which should take a snapshot of the data, and "wait" for Lucene
            assertEquals(1, manualTrinity.progressPsql());

            // Now delete all messages in PSQL
            storageAPI.deleteAllMessages();
            assertEquals(1, manualTrinity.progressPsql());

            // Make sure they are deleted in PSQL
            Future<ResponseAllMessages> allMessagesFuture = executorService.submit(storageAPI::allMessages);
            Thread.sleep(1000);

            assertEquals(1, manualTrinity.progressPsql());
            assertEquals(0, allMessagesFuture.get().getMessages().size());

            // Now allow Lucene to progress and contact PSQL
            assertEquals(3, manualTrinity.progressLucene());

            // Check that the result which we got back from PSQL is legit
            assertEquals(simeonHey, responseMessageDetailsFuture.get().getMessage());
        }
    }
}
