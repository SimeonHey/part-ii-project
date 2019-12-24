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
    
    private static final String DEFAULT_INDEX_DEST = "./luceneindex/index_output";

    private final Path indexPath = Paths.get(DEFAULT_INDEX_DEST);
    private final Analyzer analyzer = new StandardAnalyzer();

    void postMessage(Message message, Long uuid) {
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        LOGGER.info("Lucene posts message: " + message);

        Directory luceneIndexDir;
        try {
            luceneIndexDir = FSDirectory.open(indexPath);
        } catch (IOException e) {
            LOGGER.info("Error when trying to open lucene dir: " + e);
            throw new RuntimeException(e);
        }

        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        IndexWriter indexWriter;
        try {
            indexWriter = new IndexWriter(luceneIndexDir, iwc);
        } catch (IOException e) {
            LOGGER.warning("Error in Lucene when trying to open indexWriter: " + e);
            throw new RuntimeException(e);
        }

        try {
            Document doc = new Document();

            String escapedSender = QueryParser.escape(message.getSender());
            String escapedMessage = QueryParser.escape(message.getMessageText());

            LOGGER.info("Escaped sender: " + escapedSender + "; escaped message: " + escapedMessage);

            doc.add(new StringField("sender", escapedSender, Field.Store.YES));
            doc.add(new TextField("message", escapedMessage, Field.Store.NO));
            doc.add(new StoredField("uuid", uuid));

            indexWriter.addDocument(doc);
            LOGGER.info("Successfully added message " + message);
        } catch (IOException e) {
            LOGGER.warning("Error when trying to add a new doc");
            throw new RuntimeException(e);
        }

        try {
            indexWriter.flush();
            indexWriter.close();
        } catch (IOException e) {
            LOGGER.warning("Error when trying to flush and close the indexWRiter");
            throw new RuntimeException(e);
        }

        try {
            luceneIndexDir.close();
        } catch (IOException e) {
            LOGGER.warning("Error when trying to close the lucene index dir");
            throw new RuntimeException(e);
        }
    }

    List<Long> searchMessage(String searchText) {
        LOGGER.info("Searching for search text" + searchText);

        IndexReader indexReader;
        try {
            indexReader = DirectoryReader.open(FSDirectory.open(indexPath));
        } catch (IOException e) {
            LOGGER.info("Exception " + e + " when building indexReader");
            throw new RuntimeException(e);
        }
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        Analyzer analyzer = new StandardAnalyzer();
        String searchField = "message";

        QueryParser queryParser = new QueryParser(searchField, analyzer);
        Query query;
        try {
            String escaped = QueryParser.escape(searchText);
            LOGGER.info("Escaped search query: " + escaped);

            query = queryParser.parse(escaped);
        } catch (ParseException e) {
            LOGGER.info("Exception " + e + " when building query");
            throw new RuntimeException(e);
        }

        TopDocs searchResults;
        try {
            searchResults = indexSearcher.search(query, 100);
        } catch (IOException e) {
            LOGGER.info("Exception " + e + " when searching for query");
            throw new RuntimeException(e);
        }

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
    }

    void deleteAllMessages() {
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        LOGGER.info("Lucene deletes all messages");

        Directory luceneIndexDir;
        try {
            luceneIndexDir = FSDirectory.open(indexPath);
        } catch (IOException e) {
            LOGGER.info("Error when trying to open lucene dir: " + e);
            throw new RuntimeException(e);
        }

        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        IndexWriter indexWriter;
        try {
            indexWriter = new IndexWriter(luceneIndexDir, iwc);
        } catch (IOException e) {
            LOGGER.warning("Error in Lucene when trying to open indexWriter: " + e);
            throw new RuntimeException(e);
        }

        try {
            indexWriter.flush();
            indexWriter.close();
        } catch (IOException e) {
            LOGGER.warning("Error when trying to flush and close the indexWRiter");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
//        analyzer.close();
    }
}
