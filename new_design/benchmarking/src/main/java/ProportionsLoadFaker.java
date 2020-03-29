import java.util.Map;

public class ProportionsLoadFaker extends LoadFaker {
    private final static double EPS = 0.000001;

    private final Map<StupidStreamObject.ObjectType, Double> proportions;

    public ProportionsLoadFaker(int charsLimit, int wordsLimit,
                                Map<StupidStreamObject.ObjectType, Double> proportions) {
        super(charsLimit, wordsLimit);
        this.proportions = proportions;

        double sum = proportions.values().stream().reduce(0.0, Double::sum, Double::sum);
        if (sum < 1.0 - EPS || 1.0 + EPS < sum) { // Check that the sum is within one EPS of 1.0
            throw new IllegalArgumentException("Proportions must sum to 1");
        }
    }

    @Override
    void nextRequest(StorageAPI storageAPI) {
        final double current = random.nextDouble();
        double curSum = 0;
        for (Map.Entry<StupidStreamObject.ObjectType, Double> entry : proportions.entrySet()) {
            curSum += entry.getValue();

            if (curSum >= current) {
                callFromObjectType(entry.getKey(), storageAPI);
                return;
            }
        }
    }
}
