import java.io.IOException;

public class PsqlEntryPoint {
    public static void main(String[] args) throws IOException {
        PsqlUtils.PsqlInitArgs initArgs = PsqlUtils.PsqlInitArgs.defaultValues();

        LoopingConsumer<Long, StupidStreamObject> consumer =
            new LoopingConsumer<>(PsqlUtils.getConsumer(initArgs), Constants.KAFKA_CONSUME_DELAY_MS);
        PsqlStorageSystem psqlStorageSystem = PsqlUtils.getStorageSystem(initArgs);

        consumer.moveAllToLatest();
        consumer.subscribe(psqlStorageSystem);
        consumer.listenBlockingly();
    }
}
