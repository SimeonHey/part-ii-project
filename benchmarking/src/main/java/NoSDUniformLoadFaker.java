public class NoSDUniformLoadFaker extends LoadFaker {
    public NoSDUniformLoadFaker(int charsLimit, int wordsLimit) {
        super(charsLimit, wordsLimit);
    }

    @Override
    void nextRequest(PolyglotAPI polyglotAPI) {
        callFromId(random.nextInt(4), polyglotAPI);
    }
}
