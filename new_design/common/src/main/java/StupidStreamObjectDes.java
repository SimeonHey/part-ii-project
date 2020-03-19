import org.apache.kafka.common.serialization.Deserializer;

public class StupidStreamObjectDes implements Deserializer<StupidStreamObject> {
    @Override
    public StupidStreamObject deserialize(String topic, byte[] data) {
        return Constants.gson.fromJson(new String(data), StupidStreamObject.class);
    }
    // TODO: Check out Apache Commons serdes
}
