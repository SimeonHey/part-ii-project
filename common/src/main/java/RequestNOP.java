public class RequestNOP {
    public RequestNOP(StupidStreamObject stupidStreamObject) {
        if (stupidStreamObject.getObjectType() != StupidStreamObject.ObjectType.NOP) {
            throw new RuntimeException("Incorrect object type");
        }
    }

    public static StupidStreamObject toStupidStreamObject() {
        return new StupidStreamObject(StupidStreamObject.ObjectType.NOP);
    }
}
