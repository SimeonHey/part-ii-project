import java.sql.SQLException;

public class PsqlEntryPoint {
    public static void main(String[] args) throws SQLException {
        PsqlUtils.PsqlInitArgs initArgs = new PsqlUtils.PsqlInitArgs(args);

        LoopingConsumer<Long, StupidStreamObject> consumer =
            new LoopingConsumer<>(PsqlUtils.getConsumer(initArgs), Constants.KAFKA_CONSUME_DELAY_MS);
        PsqlStorageSystem psqlStorageSystem = PsqlUtils.getStorageSystem(initArgs);

        consumer.subscribe(psqlStorageSystem);
        consumer.listenBlockingly();
    }
}
