public class EntryPoint {
    public static void main(String[] args) {
        // Consume program line arguments

        // Connect to Kafka & possibly the storage api
        // Consume from Kafka topic & do things
        // Send results back to the storage api

        SubscribableConsumer<Long, StupidStreamObject> consumer =
            new SubscribableConsumer<>(KafkaUtils.createConsumer());
        LuceneWrapper luceneWrapper = new LuceneWrapper(consumer);

        consumer.listenBlockingly();
    }
}
