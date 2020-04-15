import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class BaseEventDes implements Deserializer<EventBase> {
    private Map<String, Class<? extends EventBase>> classMap;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.classMap = (Map<String, Class<? extends EventBase>>) configs.get("classMap");
    }

    @Override
    public EventBase deserialize(String topic, byte[] data) {
        return EventJsonDeserializer.deserialize(Constants.gson, new String(data), classMap);
    }
}
