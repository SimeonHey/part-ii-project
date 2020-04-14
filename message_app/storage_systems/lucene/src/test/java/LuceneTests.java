import org.apache.lucene.index.IndexReader;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class LuceneTests {
    @Test
    public void testLuceneIsSnapshotIsolated() {
        LuceneSnapshottedWrapper luceneSnapshottedWrapper =
            new LuceneSnapshottedWrapper(ConstantsMAPP.LUCENE_TEST_INDEX_DEST);
        luceneSnapshottedWrapper.deleteAllMessages(); // Clear up previous stuff

        // This message will show up in all sessions
        Message alwaysThere = new Message("simeon", "always_there");
        long alwaysThereId = 0;
        Addressable alwaysThereAddress = new Addressable("noreply", alwaysThereId);
        RequestSearchMessage requestAlwaysThere =
            new RequestSearchMessage(alwaysThereAddress, alwaysThere.getMessageText());

        // This message will show up in the default session & reader 1 but not reader 2
        Message inReader1 = new Message("simeon", "only_in_reader_1");
        long inReader1Id = 1;
        Addressable inReader1Address = new Addressable("noreply", inReader1Id);
        RequestSearchMessage requestInReader1 =
            new RequestSearchMessage(inReader1Address, inReader1.getMessageText());

        // This message will show up in the default session, but not in reader 1 nor reader 2
        Message inNone = new Message("simeon", "In_none_of_the_readers");
        long inNoneId = 2;
        Addressable inNoneAddress = new Addressable("noreply", inNoneId);
        RequestSearchMessage inNoneRequest =
            new RequestSearchMessage(inNoneAddress, inNone.getMessageText());

        luceneSnapshottedWrapper.postMessage(new RequestPostMessage(alwaysThereAddress, alwaysThere,
            ConstantsMAPP.DEFAULT_USER));

        IndexReader snapshotReader2 = luceneSnapshottedWrapper.getDefaultSnapshot(); // Reader 2 won't
        // see subsequent updates

        luceneSnapshottedWrapper.postMessage(new RequestPostMessage(inReader1Address, inReader1,
            ConstantsMAPP.DEFAULT_USER));

        IndexReader snapshotReader = luceneSnapshottedWrapper.getDefaultSnapshot(); // Reader 1 won't
        // see subsequent updates

        luceneSnapshottedWrapper.postMessage(new RequestPostMessage(inNoneAddress, inNone,
            ConstantsMAPP.DEFAULT_USER));

        // Assert alwaysThere is always there!
        assertEquals(Collections.singletonList(alwaysThereId),
            luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.getDefaultSnapshot(),
                requestAlwaysThere).getOccurrences());
        assertEquals(Collections.singletonList(alwaysThereId),
            luceneSnapshottedWrapper.searchMessage(snapshotReader,
                requestAlwaysThere).getOccurrences());
        assertEquals(Collections.singletonList(alwaysThereId),
            luceneSnapshottedWrapper.searchMessage(snapshotReader2,
                requestAlwaysThere).getOccurrences());

        // Assert inReader1 is in reader1 and the non-snapshotted version
        assertEquals(Collections.singletonList(inReader1Id),
            luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.getDefaultSnapshot(),
                requestInReader1).getOccurrences());
        assertEquals(Collections.singletonList(inReader1Id),
            luceneSnapshottedWrapper.searchMessage(snapshotReader,
                requestInReader1).getOccurrences());
        assertEquals(Collections.emptyList(),
            luceneSnapshottedWrapper.searchMessage(snapshotReader2, requestInReader1).getOccurrences());

        // Assert inNone is only in the non-snapshotted version
        assertEquals(Collections.singletonList(inNoneId),
            luceneSnapshottedWrapper.searchMessage(luceneSnapshottedWrapper.getDefaultSnapshot(),
                inNoneRequest).getOccurrences());
        assertEquals(Collections.emptyList(),
            luceneSnapshottedWrapper.searchMessage(snapshotReader, inNoneRequest).getOccurrences());
        assertEquals(Collections.emptyList(),
            luceneSnapshottedWrapper.searchMessage(snapshotReader2, inNoneRequest).getOccurrences());
    }
}
