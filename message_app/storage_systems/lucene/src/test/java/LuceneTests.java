import org.apache.lucene.index.IndexReader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class LuceneTests {
    private static final long timestamp = System.nanoTime();

    static LuceneSnapshottedSystem luceneSnapshottedWrapper;

    @BeforeClass
    public static void init() {
        luceneSnapshottedWrapper =
            new LuceneSnapshottedSystem(ConstantsMAPP.LUCENE_TEST_INDEX_DEST);
    }

    @Before
    public void beforeEach() {
        luceneSnapshottedWrapper.deleteAllMessages(); // Clear up previous stuff
    }

    @Test
    public void testLuceneIsSnapshotIsolated() {
        // This message will show up in all sessions
        Message alwaysThere = new Message("simeon", "gosho", "always_there", timestamp);
        long alwaysThereId = 0;
        Addressable alwaysThereAddress = new Addressable("noreply", alwaysThereId);
        RequestSearchMessage requestAlwaysThere =
            new RequestSearchMessage(alwaysThereAddress, alwaysThere.getMessageText());

        // This message will show up in the default session & reader 1 but not reader 2
        Message inReader1 = new Message("simeon", "gosho", "only_in_reader_1", timestamp);
        long inReader1Id = 1;
        Addressable inReader1Address = new Addressable("noreply", inReader1Id);
        RequestSearchMessage requestInReader1 =
            new RequestSearchMessage(inReader1Address, inReader1.getMessageText());

        // This message will show up in the default session, but not in reader 1 nor reader 2
        Message inNone = new Message("simeon", "gosho", "In_none_of_the_readers", timestamp);
        long inNoneId = 2;
        Addressable inNoneAddress = new Addressable("noreply", inNoneId);
        RequestSearchMessage inNoneRequest =
            new RequestSearchMessage(inNoneAddress, inNone.getMessageText());

        luceneSnapshottedWrapper.postMessage(new RequestPostMessage(alwaysThereAddress, alwaysThere));

        IndexReader snapshotReader2 = luceneSnapshottedWrapper.getMainDataView(); // Reader 2 won't
        // see subsequent updates

        luceneSnapshottedWrapper.postMessage(new RequestPostMessage(inReader1Address, inReader1));

        IndexReader snapshotReader = luceneSnapshottedWrapper.getMainDataView(); // Reader 1 won't
        // see subsequent updates

        luceneSnapshottedWrapper.postMessage(new RequestPostMessage(inNoneAddress, inNone));

        // Assert alwaysThere is always there!
        assertEquals(Collections.singletonList(alwaysThereId),
            luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.getMainDataView(),
                requestAlwaysThere).getOccurrences());
        assertEquals(Collections.singletonList(alwaysThereId),
            luceneSnapshottedWrapper.searchMessage(snapshotReader,
                requestAlwaysThere).getOccurrences());
        assertEquals(Collections.singletonList(alwaysThereId),
            luceneSnapshottedWrapper.searchMessage(snapshotReader2,
                requestAlwaysThere).getOccurrences());

        // Assert inReader1 is in reader1 and the non-snapshotted version
        assertEquals(Collections.singletonList(inReader1Id),
            luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.getMainDataView(),
                requestInReader1).getOccurrences());
        assertEquals(Collections.singletonList(inReader1Id),
            luceneSnapshottedWrapper.searchMessage(snapshotReader,
                requestInReader1).getOccurrences());
        assertEquals(Collections.emptyList(),
            luceneSnapshottedWrapper.searchMessage(snapshotReader2, requestInReader1).getOccurrences());

        // Assert inNone is only in the non-snapshotted version
        assertEquals(Collections.singletonList(inNoneId),
            luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.getMainDataView(),
                inNoneRequest).getOccurrences());
        assertEquals(Collections.emptyList(),
            luceneSnapshottedWrapper.searchMessage(snapshotReader, inNoneRequest).getOccurrences());
        assertEquals(Collections.emptyList(),
            luceneSnapshottedWrapper.searchMessage(snapshotReader2, inNoneRequest).getOccurrences());
    }

    @Test
    public void luceneDeletesMessages() {
        String mainUser = "simeon";
        String friend1 = "gosho";
        String friend2 = "bobi";

        Message message1 = new Message(mainUser, friend1, "heybro", timestamp);
        Message message2 = new Message(mainUser, friend2, "whatsup", timestamp);

        ResponseSearchMessage responseSearch1;
        ResponseSearchMessage responseSearch2;

        // Post messages and test
        luceneSnapshottedWrapper.postMessage(new RequestPostMessage(Constants.NO_RESPONSE.setChannelID(1L), message1));
        luceneSnapshottedWrapper.postMessage(new RequestPostMessage(Constants.NO_RESPONSE.setChannelID(2L), message2));
        
        IndexReader defaultSnapshot = luceneSnapshottedWrapper.getMainDataView();
        
        responseSearch1 = luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.refreshSnapshot(defaultSnapshot),
                new RequestSearchMessage(Constants.NO_RESPONSE.setChannelID(1L), message1.getMessageText()));
        responseSearch2 = luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.refreshSnapshot(defaultSnapshot),
                new RequestSearchMessage(Constants.NO_RESPONSE.setChannelID(2L), message2.getMessageText()));

        assertEquals(1, responseSearch1.getOccurrences().size());
        assertEquals(1, responseSearch2.getOccurrences().size());

        // Delete one convo and test
        luceneSnapshottedWrapper.deleteConvoThread(
            new RequestDeleteConversation(Constants.NO_RESPONSE, mainUser, friend1));

        responseSearch1 = luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.refreshSnapshot(defaultSnapshot),
                new RequestSearchMessage(Constants.NO_RESPONSE.setChannelID(1L), message1.getMessageText()));
        responseSearch2 = luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.refreshSnapshot(defaultSnapshot),
                new RequestSearchMessage(Constants.NO_RESPONSE.setChannelID(2L), message2.getMessageText()));

        assertEquals(0, responseSearch1.getOccurrences().size());
        assertEquals(1, responseSearch2.getOccurrences().size());


        // Delete all and test
        luceneSnapshottedWrapper.deleteAllMessages();

        responseSearch1 = luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.refreshSnapshot(defaultSnapshot),
                new RequestSearchMessage(Constants.NO_RESPONSE.setChannelID(1L), message1.getMessageText()));
        responseSearch2 = luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.refreshSnapshot(defaultSnapshot),
                new RequestSearchMessage(Constants.NO_RESPONSE.setChannelID(2L), message2.getMessageText()));

        assertEquals(0, responseSearch1.getOccurrences().size());
        assertEquals(0, responseSearch2.getOccurrences().size());
    }
}
