import java.io.Serializable;
import java.util.HashMap;

public class StupidStreamObject implements Serializable {
    public enum ObjectType {
        POST_MESSAGE
    }

    private ObjectType objectType;
    private HashMap<String, String> properties;

    public StupidStreamObject(ObjectType objectType) {
        this.properties = new HashMap<>();
        this.objectType = objectType;
    }

    public StupidStreamObject setProperty(String name, String property) {
        properties.put(name, property);
        return this;
    }

    public String getProperty(String name) {
        return properties.get(name);
    }

    public ObjectType getObjectType() {
        return this.objectType;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }
}