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
import java.util.Arrays;

/**
 * Proxy for Lucene transactions
 */
public class LuceneWrapper {
    private static final String DEFAULT_INDEX_DEST = "./luceneindex/index_output";

    // TODO : Separate as dependencies
    private final Path indexPath = Paths.get(DEFAULT_INDEX_DEST);
    private final Analyzer analyzer = new StandardAnalyzer();

    public void postMessage(PostMessageRequest postMessageRequest, Long uuid) {
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        System.out.println("Lucene posts message: " + postMessageRequest);

        Directory luceneIndexDir;
        try {
            luceneIndexDir = FSDirectory.open(indexPath);
        } catch (IOException e) {
            // TODO LOGGING
            throw new RuntimeException(e);
        }

        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        IndexWriter indexWriter;
        try {
            indexWriter = new IndexWriter(luceneIndexDir, iwc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            Document doc = new Document();
            doc.add(new StringField("sender", postMessageRequest.getSender(), Field.Store.YES));
            doc.add(new TextField("message", postMessageRequest.getMessageText(), Field.Store.NO));
            doc.add(new StoredField("uuid", uuid));

            indexWriter.addDocument(doc);
            System.out.println("Successfully added message " + postMessageRequest.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            indexWriter.flush();
            indexWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SearchMessageResponse searchMessage(SearchMessageRequest searchMessageRequest) {
        IndexReader indexReader;
        try {
            indexReader = DirectoryReader.open(FSDirectory.open(indexPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        Analyzer analyzer = new StandardAnalyzer();
        String searchField = "message";

        QueryParser queryParser = new QueryParser(searchField, analyzer);
        Query query;
        try {
            query = queryParser.parse(searchMessageRequest.getSearchText());
        } catch (ParseException e) {
            throw new RuntimeException(e); // TODO
        }

        TopDocs searchResults;
        try {
            searchResults = indexSearcher.search(query, 100);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SearchMessageResponse response = new SearchMessageResponse();

        Arrays.stream(searchResults.scoreDocs).forEach(scoreDoc -> {
            try {
                String res = indexSearcher.doc(scoreDoc.doc).get("uuid");
                response.addOccurrence(Long.valueOf(res));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        System.out.println("Lucene search for message " + searchMessageRequest + " and got " + response);
        return response;
    }
}
