import java.util.ArrayList;
import java.util.List;

public class ResponseAllMessages {
    private List<Message> messages;

    public void addMessage(Message message) {
        messages.add(message);
    }

    public List<Message> getMessages() {
        return messages;
    }

    public ResponseAllMessages(List<Message> messages) {
        this.messages = messages;
    }

    public ResponseAllMessages() {
        this.messages = new ArrayList<>();
    }

    public StupidStreamObject toStupidStreamObject() {
        var res = new StupidStreamObject(StupidStreamObject.ObjectType.GET_ALL_MESSAGES, Constants.NO_RESPONSE)
            .setProperty("size", String.valueOf(messages.size()));

        for (int i=0; i<messages.size(); i++) {
            // TODO: Deserialize properly
            res.setProperty(String.valueOf(i), messages.get(i).getSender() + " : " + messages.get(i).getMessageText());
        }

        return res;
    }

    @Override
    public String toString() {
        return "ResponseAllMessages{" +
            "messages size = " + messages.size() +
            "\nmessages: " + messages +
            '}';
    }
}
