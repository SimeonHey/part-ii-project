public class PostOnlyLoadFaker extends LoadFaker {
    public PostOnlyLoadFaker(int charsLimit, int wordsLimit) {
        super(charsLimit, wordsLimit);
    }

    @Override
    void nextRequest(StorageAPI storageAPI) {
        callFromId(0, storageAPI);
    }
}
