import java.util.ArrayList;
import java.util.List;

public class SearchMessageResponse {
    private final List<Long> occurrences;

    public SearchMessageResponse() {
        this.occurrences = new ArrayList<>();
    }

    public SearchMessageResponse addOccurrence(Long uuid) {
        this.occurrences.add(uuid);
        return this;
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
