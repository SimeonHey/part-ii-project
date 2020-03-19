import org.apache.kafka.common.serialization.Serializer;

public class StupidStreamObjectSer implements Serializer<StupidStreamObject> {
    @Override
    public byte[] serialize(String topic, StupidStreamObject data) {
        return Constants.gson.toJson(data).getBytes();
    }
}
