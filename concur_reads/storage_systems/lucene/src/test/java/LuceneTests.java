import org.apache.lucene.index.IndexReader;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class LuceneTests {
    @Test
    public void testLuceneIsSnapshotIsolated() {
        LuceneWrapper luceneWrapper = new LuceneWrapper();
        Message alwaysThere = new Message("simeon", "always_there");
        long alwaysThereId = 0;

        Message inReader1 = new Message("simeon", "only_in_reader_1");
        long inReader1Id = 1;

        Message inNone = new Message("simeon", "In_none_of_the_readers");
        long inNoneId = 2;

        luceneWrapper.postMessage(alwaysThere, alwaysThereId);

        IndexReader snapshotReader2 = luceneWrapper.newSnapshotReader(); // Reader 2 won't see subsequent updates

        luceneWrapper.postMessage(inReader1, inReader1Id);

        IndexReader snapshotReader = luceneWrapper.newSnapshotReader(); // Reader 1 won't see subsequent updates

        luceneWrapper.postMessage(inNone, inNoneId);

        // Assert alwaysThere is always there!
        assertEquals(Collections.singletonList(alwaysThereId),
            luceneWrapper.searchMessage(alwaysThere.getMessageText()));
        assertEquals(Collections.singletonList(alwaysThereId),
            luceneWrapper.searchMessage(snapshotReader, alwaysThere.getMessageText()));
        assertEquals(Collections.singletonList(alwaysThereId),
            luceneWrapper.searchMessage(snapshotReader2, alwaysThere.getMessageText()));

        // Assert inReader1 is in reader1 and the non-snapshotted version
        assertEquals(Collections.singletonList(inReader1Id),
            luceneWrapper.searchMessage(inReader1.getMessageText()));
        assertEquals(Collections.singletonList(inReader1Id),
            luceneWrapper.searchMessage(snapshotReader, inReader1.getMessageText()));
        assertEquals(Collections.emptyList(),
            luceneWrapper.searchMessage(snapshotReader2, inReader1.getMessageText()));

        // Assert inNone is only in the non-snapshotted version
        assertEquals(Collections.singletonList(inNoneId),
            luceneWrapper.searchMessage(inNone.getMessageText()));
        assertEquals(Collections.emptyList(),
            luceneWrapper.searchMessage(snapshotReader, inNone.getMessageText()));
        assertEquals(Collections.emptyList(),
            luceneWrapper.searchMessage(snapshotReader2, inNone.getMessageText()));
    }
}
