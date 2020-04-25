import java.util.List;

public class ResponseSearchMessage {
    private final List<Long> occurrences;

    public ResponseSearchMessage(List<Long> occurrences) {
        this.occurrences = occurrences;
    }

    public List<Long> getOccurrences() {
        return occurrences;
    }

    @Override
    public String toString() {
        return "ResponseSearchMessage{" +
            "occurrences=" + occurrences +
            '}';
    }
}
