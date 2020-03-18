public class PostOnlyLoadFaker extends LoadFaker {
    @Override
    void nextRequest(StorageAPI storageAPI) {
        callFromId(0, storageAPI);
    }
}
