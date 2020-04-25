public class SdBiasedLoadFaker extends LoadFaker {
    private static final double SD_CHANCE = 0.9;

    public SdBiasedLoadFaker(int charsLimit, int wordsLimit) {
        super(charsLimit, wordsLimit);
    }

    @Override
    void nextRequest(PolyglotAPI polyglotAPI) {
        if (random.nextDouble() < SD_CHANCE) {
            callFromObjectType(RequestSearchAndGetDetails.class.getName(), polyglotAPI);
        } else {
            callFromId(random.nextInt(5), polyglotAPI);
        }
    }
}
