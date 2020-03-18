import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class PsqlEntryPoint {
    private final static Logger LOGGER = Logger.getLogger(PsqlEntryPoint.class.getName());

    public static void main(String[] args) throws IOException, ParseException {
        Options options = new Options();
        options.addOption("port", true, "Listening port of the wrapper");
        options.addOption("kafka", true, "Kafka address");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        int port = Integer.parseInt(commandLine.getOptionValue("port", String.valueOf(Constants.PSQL_LISTEN_PORT)));
        String kafkaAddress = commandLine.getOptionValue("kafka");

        LOGGER.info("Starting a PSQL wrapper on port " + port + ", and kafka address " + kafkaAddress);

        var loopingConsumer =
            new LoopingConsumer<>(KafkaUtils.createConsumer("psql", kafkaAddress, Constants.KAFKA_TOPIC));
        var psqlFactory = new PsqlStorageSystemsFactory(loopingConsumer, port);

        psqlFactory.concurReads();
        psqlFactory.listenBlockingly(Executors.newFixedThreadPool(1));
    }
}
