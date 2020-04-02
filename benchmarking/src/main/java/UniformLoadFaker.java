public class UniformLoadFaker extends LoadFaker {
    public UniformLoadFaker(int charsLimit, int wordsLimit) {
        super(charsLimit, wordsLimit);
    }

    @Override
    public void nextRequest(StorageAPI storageAPI) {
        callFromId(random.nextInt(5), storageAPI);
    }
}
