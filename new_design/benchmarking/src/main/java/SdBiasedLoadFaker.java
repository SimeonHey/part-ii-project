public class SdBiasedLoadFaker extends LoadFaker {
    private static final double SD_CHANCE = 0.9;

    public SdBiasedLoadFaker(int charsLimit, int wordsLimit) {
        super(charsLimit, wordsLimit);
    }

    @Override
    void nextRequest(StorageAPI storageAPI) {
        if (random.nextDouble() < SD_CHANCE) {
            callFromObjectType(StupidStreamObject.ObjectType.SEARCH_AND_DETAILS, storageAPI);
        } else {
            callFromId(random.nextInt(5), storageAPI);
        }
    }
}