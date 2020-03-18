import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.util.concurrent.Executors;

public class LuceneEntryPoint {
    public static void main(String[] args) throws IOException, ParseException {
        Options options = new Options();
        options.addOption("psql", true, "PSQL address");
        options.addOption("kafka", true, "Kafka address");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        String psqlAddress = commandLine.getOptionValue("psql");
        String kafkaAddress = commandLine.getOptionValue("kafka");

        var luceneFactory =
            new LuceneStorageSystemFactory(LoopingConsumer.fresh("lucene", kafkaAddress), psqlAddress);
        luceneFactory.simpleOlep();
        luceneFactory.listenBlockingly(Executors.newFixedThreadPool(1));
    }
}
