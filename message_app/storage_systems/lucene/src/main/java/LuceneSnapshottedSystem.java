import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Proxy for Lucene transactions
 */
class LuceneSnapshottedSystem extends SnapshottedStorageSystem<IndexReader>
    implements MessageAppDatabase<IndexReader>{

    private static final Logger LOGGER = Logger.getLogger(LuceneSnapshottedSystem.class.getName());
    private static final int MAX_CONNECTIONS = Integer.MAX_VALUE;

    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_SENDER = "sender";
    private static final String FIELD_UUID = "uuid";

    private final Path indexPath;
    private final Analyzer analyzer = new StandardAnalyzer();

    public LuceneSnapshottedSystem() {
        super(MAX_CONNECTIONS);
        indexPath = Paths.get(ConstantsMAPP.LUCENE_DEFAULT_INDEX_DEST);
    }

    public LuceneSnapshottedSystem(String indexDestination) {
        super(MAX_CONNECTIONS);
        indexPath = Paths.get(indexDestination);
    }

    // Write requests
    @Override
    public void postMessage(RequestPostMessage requestPostMessage) {
        LOGGER.info("Lucene posts message: " + requestPostMessage.getMessage());
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        try (Directory luceneIndexDir = FSDirectory.open(indexPath);
             IndexWriter indexWriter = new IndexWriter(luceneIndexDir, iwc)) {
            Document doc = new Document();

            String escapedSender = QueryParser.escape(requestPostMessage.getMessage().getSender());
            String escapedMessage = QueryParser.escape(requestPostMessage.getMessage().getMessageText());

//            LOGGER.info("Escaped sender: " + escapedSender + "; escaped message: " + escapedMessage);

            doc.add(new StringField(FIELD_SENDER, escapedSender, Field.Store.YES));
            doc.add(new TextField(FIELD_MESSAGE, escapedMessage, Field.Store.NO));
            doc.add(new StoredField(FIELD_UUID, requestPostMessage.getResponseAddress().getChannelID()));

            indexWriter.addDocument(doc);
        } catch (IOException e) {
            LOGGER.warning("Error when posting message: " + e);
            throw new RuntimeException(e);
        }
    }

    public void deleteAllMessages() {
        LOGGER.info("Lucene deletes all messages");

        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        try (Directory luceneIndexDir = FSDirectory.open(indexPath);
             IndexWriter ignored = new IndexWriter(luceneIndexDir, iwc)) {
        } catch (IOException e) {
            LOGGER.warning("Error when posting message: " + e);
            throw new RuntimeException(e);
        }
    }

    // Read requests
    @Override
    public ResponseSearchMessage searchMessage(IndexReader snapshot,
                                               RequestSearchMessage searchMessage) {
        try (Analyzer analyzer = new StandardAnalyzer()) {
            // change the analyzer if you want different tokenization or filters
            IndexSearcher indexSearcher = new IndexSearcher(snapshot);

            QueryParser queryParser = new QueryParser(FIELD_MESSAGE, analyzer);
            String escaped = QueryParser.escape(searchMessage.getSearchText());
            Query query = queryParser.parse(escaped);

            TopDocs searchResults = indexSearcher.search(query, 100);

            List<Long> occurrences = new ArrayList<>();

            Arrays.stream(searchResults.scoreDocs).forEach(scoreDoc -> {
                try {
                    String res = indexSearcher.doc(scoreDoc.doc).get("uuid");
                    occurrences.add(Long.valueOf(res));
                } catch (IOException e) {
                    LOGGER.info("Exception " + e + " when analysing search result");
                    throw new RuntimeException(e);
                }
            });

            LOGGER.info("Lucene searched for text of length " + searchMessage.getSearchText().length() +
                " and got " + occurrences);
            return new ResponseSearchMessage(occurrences);
        } catch (IOException | ParseException e) {
            LOGGER.warning("Error when performing search: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResponseMessageDetails getMessageDetails(IndexReader snapshot, RequestMessageDetails requestMessageDetails) {
        throw new RuntimeException("Lucene doesn't have message details functionality implemented");
    }

    @Override
    public ResponseAllMessages getAllMessages(IndexReader snapshot, RequestAllMessages requestAllMessages) {
        throw new RuntimeException("Lucene doesn't have get all messages functionality implemented");
    }

    @Override
    IndexReader freshConcurrentSnapshot() {
        try {
            return DirectoryReader.open(FSDirectory.open(indexPath));
        } catch (IOException e) {
            LOGGER.warning("Error when trying to open a new snapshot reader: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexReader getDefaultSnapshot() {
        return freshConcurrentSnapshot();
    }

    @Override
    IndexReader refreshSnapshot(IndexReader bareSnapshot) {
        try {
            bareSnapshot.close();
        } catch (IOException e) {
            LOGGER.warning("Error when trying to close a snapshot when refreshing it with Lucene: " + e);
            throw new RuntimeException(e);
        }

        return freshConcurrentSnapshot();
    }

    @Override
    public void close() throws Exception {
//        analyzer.close();
    }
}
