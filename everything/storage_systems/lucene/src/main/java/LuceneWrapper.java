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
class LuceneWrapper implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(LuceneWrapper.class.getName());

    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_SENDER = "sender";
    private static final String FIELD_UUID = "uuid";

    private final Path indexPath;
    private final Analyzer analyzer = new StandardAnalyzer();

    public LuceneWrapper(String indexDestination) {
        indexPath = Paths.get(indexDestination);
    }

    // Write requests

    void postMessage(Message message, Long uuid) {
        LOGGER.info("Lucene posts message: " + message);
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        try (Directory luceneIndexDir = FSDirectory.open(indexPath);
             IndexWriter indexWriter = new IndexWriter(luceneIndexDir, iwc)) {
            Document doc = new Document();

            String escapedSender = QueryParser.escape(message.getSender());
            String escapedMessage = QueryParser.escape(message.getMessageText());

            LOGGER.info("Escaped sender: " + escapedSender + "; escaped message: " + escapedMessage);

            doc.add(new StringField(FIELD_SENDER, escapedSender, Field.Store.YES));
            doc.add(new TextField(FIELD_MESSAGE, escapedMessage, Field.Store.NO));
            doc.add(new StoredField(FIELD_UUID, uuid));

            indexWriter.addDocument(doc);
        } catch (IOException e) {
            LOGGER.warning("Error when posting message: " + e);
            throw new RuntimeException(e);
        }
    }

    void deleteAllMessages() {
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

    List<Long> searchMessage(String searchText) {
        LOGGER.info("Searching in the latest snapshot for search text" + searchText);

        try (IndexReader indexReader = this.newSnapshotReader()) {
            return searchMessage(indexReader, searchText);
        } catch (IOException e) {
            LOGGER.warning("Error when performing search: " + e);
            throw new RuntimeException(e);
        }
    }

    List<Long> searchMessage(IndexReader indexReader, String searchText) {
        LOGGER.info("Searching in a previous snapshot for search text" + searchText);

        try (Analyzer analyzer = new StandardAnalyzer()) {
            // change the analyzer if you want different tokenization or filters
            IndexSearcher indexSearcher = new IndexSearcher(indexReader);

            QueryParser queryParser = new QueryParser(FIELD_MESSAGE, analyzer);
            String escaped = QueryParser.escape(searchText);
            LOGGER.info("Escaped search query: " + escaped);
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

            LOGGER.info("Lucene search for text " + searchText + " and got " + occurrences);
            return occurrences;
        } catch (IOException | ParseException e) {
            LOGGER.warning("Error when performing search: " + e);
            throw new RuntimeException(e);
        }
    }

    IndexReader newSnapshotReader() {
        try {
            return DirectoryReader.open(FSDirectory.open(indexPath));
        } catch (IOException e) {
            LOGGER.warning("Error when trying to open a new snapshot reader: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
//        analyzer.close();
    }
}
