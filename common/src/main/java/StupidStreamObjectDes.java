import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class StupidStreamObjectDes implements Deserializer<StupidStreamObject> {
    private Gson gson = new Gson();

    @Override
    public StupidStreamObject deserialize(String topic, byte[] data) {
        return gson.fromJson(new String(data), StupidStreamObject.class);
    }
    // TODO: Check out Apache Commons serdes
}
