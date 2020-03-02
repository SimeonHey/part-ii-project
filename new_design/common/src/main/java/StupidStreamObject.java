import java.io.Serializable;
import java.util.HashMap;

/**
 * Basically a serialized version of an object, represented by a hashmap of properties (customizable by each message
 * type) and an object type
 */
public class StupidStreamObject implements Serializable {
    public enum ObjectType {
        POST_MESSAGE,
        SEARCH_MESSAGES,
        DELETE_ALL_MESSAGES,
        NOP,
        GET_MESSAGE_DETAILS,
        GET_ALL_MESSAGES,
        SEARCH_AND_DETAILS
    }

    private final ObjectType objectType;
    private final HashMap<String, String> properties = new HashMap<>();

    private final Addressable responseAddress;

    public StupidStreamObject(ObjectType objectType, Addressable responseAddress) {
        this.objectType = objectType;
        this.responseAddress = responseAddress;
    }

    StupidStreamObject setProperty(String name, String property) {
        properties.put(name, property);
        return this;
    }

    String getProperty(String name) {
        return properties.get(name);
    }

    public ObjectType getObjectType() {
        return this.objectType;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }

    public Addressable getResponseAddress() {
        return responseAddress;
    }
}