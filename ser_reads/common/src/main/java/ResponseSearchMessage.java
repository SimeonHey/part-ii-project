import java.util.ArrayList;
import java.util.List;

public class ResponseSearchMessage {
    private final List<Long> occurrences;

    public ResponseSearchMessage() {
        this.occurrences = new ArrayList<>();
    }

    public void addOccurrence(Long uuid) {
        this.occurrences.add(uuid);
    }

    public List<Long> getOccurrences() {
        return occurrences;
    }

    @Override
    public String toString() {
        return "SearchMessageResponse{" +
            "occurrences=" + occurrences.toString() +
            '}';
    }
}
