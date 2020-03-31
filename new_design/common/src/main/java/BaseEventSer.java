import org.apache.kafka.common.serialization.Serializer;

public class BaseEventSer implements Serializer<BaseEvent> {
    @Override
    public byte[] serialize(String topic, BaseEvent data) {
        return Constants.gson.toJson(data).getBytes();
    }
}
