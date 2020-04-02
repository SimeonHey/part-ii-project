import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class BaseEventDes implements Deserializer<BaseEvent> {
    private Map<String, Class<? extends BaseEvent>> classMap;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.classMap = (Map<String, Class<? extends BaseEvent>>) configs.get("classMap");
    }

    @Override
    public BaseEvent deserialize(String topic, byte[] data) {
        return EventJsonDeserializer.deserialize(Constants.gson, new String(data), classMap);
    }
}
