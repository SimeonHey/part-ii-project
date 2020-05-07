import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import java.util.Map;
import java.util.logging.Logger;

public class EventJsonDeserializer {
    private final static Logger LOGGER = Logger.getLogger(EventJsonDeserializer.class.getName());

    static EventBase deserialize(Gson gson, String serializedEvent, Map<String, Class<? extends EventBase>> classMap)
        throws JsonParseException {
        JsonElement jsonElement = JsonParser.parseString(serializedEvent);
        try {
            String objectType = jsonElement.getAsJsonObject().get("objectType").getAsString();
            Class<? extends EventBase> objectClass = classMap.get(objectType);

            if (objectClass == null) {
                LOGGER.warning("Unknown object type " + objectType +
                    ", will treat it as BaseEvent. Class map: " + classMap);
                return gson.fromJson(jsonElement, EventBase.class);
            }

            return gson.fromJson(jsonElement, objectClass);
        } catch (Exception e) {
            LOGGER.warning("Error in Kafka deserialization: " + e);
            throw new RuntimeException(e);
        }
    }
}
