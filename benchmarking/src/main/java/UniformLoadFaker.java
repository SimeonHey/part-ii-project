public class UniformLoadFaker extends LoadFaker {
    public UniformLoadFaker(int charsLimit, int wordsLimit) {
        super(charsLimit, wordsLimit);
    }

    @Override
    public void nextRequest(PolyglotAPI polyglotAPI) {
        callFromId(random.nextInt(Events.values().length), polyglotAPI);
    }
}
