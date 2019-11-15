public class StupidStreamObjectFactory {
    public static StupidStreamObject getPostMessageRequest(String sender, String message) {
        StupidStreamObject stupidStreamObject = new StupidStreamObject(StupidStreamObject.ObjectType.POST_MESSAGE);
        stupidStreamObject.setProperty("sender", sender);
        stupidStreamObject.setProperty("message", message);
        return stupidStreamObject;
    }
}
