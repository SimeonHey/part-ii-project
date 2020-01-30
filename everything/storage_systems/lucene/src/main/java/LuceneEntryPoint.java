public class LuceneEntryPoint {
    public static void main(String[] args) {
        LuceneUtils.LuceneInitArgs initArgs = LuceneUtils.LuceneInitArgs.defaultValues();

        LoopingConsumer<Long, StupidStreamObject> loopingConsumer =
            new LoopingConsumer<>(LuceneUtils.getConsumer(initArgs), Constants.KAFKA_CONSUME_DELAY_MS);

        LuceneStorageSystem luceneStorageSystem = LuceneUtils.getStorageSystem(initArgs);

        loopingConsumer.moveAllToLatest();
        loopingConsumer.subscribe(luceneStorageSystem);
        loopingConsumer.listenBlockingly();
    }
}
