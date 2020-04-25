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
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
    private static final String FIELD_RECIPIENT = "recipient";
    private static final String FIELD_MESSAGE_ID = "uuid";

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
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        try (Directory luceneIndexDir = FSDirectory.open(indexPath);
             IndexWriter indexWriter = new IndexWriter(luceneIndexDir, iwc)) {
            Document doc = new Document();

            String escapedSender = QueryParser.escape(requestPostMessage.getMessage().getSender());
            String escapedRecipient = QueryParser.escape(requestPostMessage.getMessage().getRecipient());
            String escapedMessage = QueryParser.escape(requestPostMessage.getMessage().getMessageText());

//            LOGGER.info("Escaped sender: " + escapedSender + "; escaped message: " + escapedMessage);

            doc.add(new StringField(FIELD_SENDER, escapedSender, Field.Store.YES));
            doc.add(new StringField(FIELD_RECIPIENT, escapedRecipient, Field.Store.YES));
            doc.add(new TextField(FIELD_MESSAGE, escapedMessage, Field.Store.NO));
            doc.add(new StoredField(FIELD_MESSAGE_ID, requestPostMessage.getResponseAddress().getChannelID()));

            indexWriter.addDocument(doc);
            indexWriter.commit();
            LOGGER.info("Lucene posted message: " + requestPostMessage.getMessage());
        } catch (IOException e) {
            LOGGER.warning("Error when posting message: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteAllMessages() {

        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        try (Directory luceneIndexDir = FSDirectory.open(indexPath);
             IndexWriter indexWriter = new IndexWriter(luceneIndexDir, iwc)) {
            indexWriter.commit();
            LOGGER.info("Lucene deleted all messages");
        } catch (IOException e) {
            LOGGER.warning("Error when deleteing all messages: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteConvoThread(RequestDeleteConversation deleteConversation) {
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        try (Directory luceneIndexDir = FSDirectory.open(indexPath);
             IndexWriter indexWriter = new IndexWriter(luceneIndexDir, iwc)) {

            // Either user1 is the SENDER, user2 is the RECIPIENT
            BooleanQuery.Builder builder1 = new BooleanQuery.Builder();
            builder1.add(new TermQuery(new Term(FIELD_SENDER, deleteConversation.getUser1())),
                BooleanClause.Occur.MUST);
            builder1.add(new TermQuery(new Term(FIELD_RECIPIENT, deleteConversation.getUser2())),
                BooleanClause.Occur.MUST);

            // Or user2 is the SENDER, user1 is the RECIPIENT
            BooleanQuery.Builder builder2 = new BooleanQuery.Builder();
            builder2.add(new TermQuery(new Term(FIELD_SENDER, deleteConversation.getUser2())),
                BooleanClause.Occur.MUST);
            builder2.add(new TermQuery(new Term(FIELD_RECIPIENT, deleteConversation.getUser1())),
                BooleanClause.Occur.MUST);

            // Delete documents in which either of the terms exists
            indexWriter.deleteDocuments(builder1.build(), builder2.build());
            indexWriter.commit();
            LOGGER.info("Lucene deleted messages in conversations: " + deleteConversation);
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
                    String res = indexSearcher.doc(scoreDoc.doc).get(FIELD_MESSAGE_ID);
                    occurrences.add(Long.valueOf(res));
                } catch (IOException e) {
                    LOGGER.info("Exception " + e + " when analysing search result");
                    throw new RuntimeException(e);
                }
            });

            LOGGER.info("Lucene searched for text " + searchMessage.getSearchText() + " and got " + occurrences);
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
    public ResponseAllMessages getConvoMessages(IndexReader snapshot, RequestConvoMessages requestConvoMessages) {
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
