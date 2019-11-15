import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

public class StupidStreamObjectSer implements Serializer<StupidStreamObject> {
    private Gson gson = new Gson();

    @Override
    public byte[] serialize(String topic, StupidStreamObject data) {
        return gson.toJson(data).getBytes();
    }
}
