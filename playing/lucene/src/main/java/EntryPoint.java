import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class EntryPoint {
    public static final String MY_INDEX_STUFF = "./to_index";
    public static final String DEFAULT_INDEX_DEST = "./luceneindex/index_output";
    public static final Boolean SHOULD_INDEX = true;

    public static void indexDocsTo(String docsStringPath, String indexTo) throws IOException {
        final Path docsPath = Paths.get(docsStringPath);
        final Path indexPath = Paths.get(indexTo);

        Directory luceneIndexDir = FSDirectory.open(indexPath);

        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        IndexWriter indexWriter = new IndexWriter(luceneIndexDir, iwc);

        Files.walk(docsPath)
            .filter(Files::isRegularFile)
            .forEach((Path file) -> {
                try (InputStream inputStream = Files.newInputStream(file)) {
                    Document doc = new Document();
                    doc.add(new StringField("path", file.toString(), Field.Store.YES));
                    doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(inputStream))));

                    indexWriter.addDocument(doc);
                    System.out.println("Successfully added file " + file.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

        indexWriter.close();
    }

    public static List<String> getTopHits(String indexDir, String toSearchFor) throws IOException, ParseException {


        final Path indexPath = Paths.get(indexDir);
        IndexReader indexReader = DirectoryReader.open(FSDirectory.open(indexPath));
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        Analyzer analyzer = new StandardAnalyzer();
        String searchField = "contents";

        QueryParser queryParser = new QueryParser(searchField, analyzer);
        Query query = queryParser.parse(toSearchFor);

        TopDocs searchResults = indexSearcher.search(query, 100);
        return Arrays.stream(searchResults.scoreDocs).map(scoreDoc -> {
            String res = "N/A";
            try {
                res = indexSearcher.doc(scoreDoc.doc).toString();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return res;
        }).collect(Collectors.toList());
    }

    public static void main(String[] args) throws IOException, ParseException {
        String indexTo = DEFAULT_INDEX_DEST;

        if (SHOULD_INDEX) {
            indexDocsTo(MY_INDEX_STUFF, indexTo);
            System.out.println("Indexing finished");
        } else {
            System.out.println("Skipping indexing");
        }

        Scanner in = new Scanner(System.in);

        while (true) {
            System.out.println("\nSearch query: ");

            String toSearchFor;
            try {
                toSearchFor = in.nextLine();
            } catch (Exception e) {
                break;
            }

            getTopHits(indexTo, toSearchFor).forEach(System.out::println);
        }

        System.out.println("Terminating lucene");
    }
}
