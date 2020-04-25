public class PostOnlyLoadFaker extends LoadFaker {
    public PostOnlyLoadFaker(int charsLimit, int wordsLimit) {
        super(charsLimit, wordsLimit);
    }

    @Override
    void nextRequest(PolyglotAPI polyglotAPI) {
        callFromId(0, polyglotAPI);
    }
}
