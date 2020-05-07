import org.apache.kafka.common.serialization.Serializer;

public class BaseEventSer implements Serializer<EventBase> {
    @Override
    public byte[] serialize(String topic, EventBase data) {
        return Constants.gson.toJson(data).getBytes();
    }
}
