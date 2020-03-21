public class NoSDUniformLoadFaker extends LoadFaker {
    public NoSDUniformLoadFaker(int charsLimit, int wordsLimit) {
        super(charsLimit, wordsLimit);
    }

    @Override
    void nextRequest(StorageAPI storageAPI) {
        callFromId(random.nextInt(4), storageAPI);
    }
}
