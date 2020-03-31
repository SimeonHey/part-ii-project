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

    @Override
    public String toString() {
        return "ResponseAllMessages{" +
            "messages size = " + messages.size() +
            "\nmessages: " + messages +
            '}';
    }
}
