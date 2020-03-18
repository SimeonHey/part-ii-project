public class NonDeleteUniformLoadFaker extends LoadFaker {
    @Override
    public void nextRequest(StorageAPI storageAPI) {
        callFromId(random.nextInt(5), storageAPI);
    }
}
