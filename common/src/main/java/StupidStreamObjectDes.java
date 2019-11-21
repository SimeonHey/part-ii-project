import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

public class StupidStreamObjectDes implements Deserializer<StupidStreamObject> {
    private final Gson gson = new Gson();

    @Override
    public StupidStreamObject deserialize(String topic, byte[] data) {
        return gson.fromJson(new String(data), StupidStreamObject.class);
    }
    // TODO: Check out Apache Commons serdes
}
