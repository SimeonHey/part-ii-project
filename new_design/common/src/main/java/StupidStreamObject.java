import java.io.Serializable;
import java.util.HashMap;

/**
 * Basically a serialized version of an object, represented by a hashmap of properties (customizable by each message
 * type) and an object type. TODO: Why not JSON?
 */
public class StupidStreamObject implements Serializable {
    public enum ObjectType {
        POST_MESSAGE,
        GET_ALL_MESSAGES,
        SEARCH_MESSAGES,
        GET_MESSAGE_DETAILS,
        SEARCH_AND_DETAILS,
        DELETE_ALL_MESSAGES,
        NOP
    }

    private final ObjectType objectType;
    private final Addressable responseAddress;

    private final HashMap<String, String> properties = new HashMap<>();

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