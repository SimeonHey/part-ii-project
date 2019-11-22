import javax.swing.*;
import java.awt.*;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

class FrontEnd extends JFrame {
    private static final Logger LOGGER = Logger.getLogger(FrontEnd.class.getName());
    private Consumer<Message> messageConsumer;
    private TextField messageField;
    private JTextArea messageDisplay;
    private String sender;
    private Runnable refreshCallback;

    FrontEnd(String sender) {
        this.sender = sender;
        this.messageConsumer = null;
        initUI();
    }

    void addPostCallback(Consumer<Message> messageConsumer) {
        this.messageConsumer = messageConsumer;
        this.setVisible(true);
    }

    void addRefreshCallback(Runnable refreshCallback) {
        this.refreshCallback = refreshCallback;
    }

    private void addMessage(Message message) {
        String currentText = this.messageDisplay.getText();
        String newText = currentText +
            String.format("\n%s said: %s", message.getSender(), message.getMessageText());
        this.messageDisplay.setText(newText);
    }

    void setMessages(List<Message> messages) {
        this.messageDisplay.setText("");
        messages.forEach(this::addMessage);
    }

    private void sendCurrentMessage() {
        LOGGER.info("The button was pressed, and the text is " +
            messageField.getText());

        Message message = new Message(this.sender, messageField.getText());
        this.messageConsumer.accept(message);
        this.messageField.setText("");
    }

    private JPanel initMessageControls() {
        JPanel jPanel = new JPanel();
        jPanel.setLayout(new BorderLayout());

        this.messageField = new TextField();
        this.messageField.addActionListener(e -> sendCurrentMessage());

        jPanel.add(messageField, BorderLayout.CENTER);

        var sendButton = new JButton("Send message");
        sendButton.addActionListener((event) -> sendCurrentMessage());
        jPanel.add(sendButton, BorderLayout.EAST);

        return jPanel;
    }

    private void initUI() {
        this.setLayout(new BorderLayout());

        this.messageDisplay = new JTextArea();
        messageDisplay.setEditable(false);

        JScrollPane scrollMessageDisplay = new JScrollPane(this.messageDisplay);
        this.add(scrollMessageDisplay, BorderLayout.CENTER);

        JPanel messageControls = initMessageControls();
        this.add(messageControls, BorderLayout.SOUTH);

        JButton refreshButton = new JButton("Refresh thread");
        refreshButton.addActionListener(e -> this.refreshCallback.run());
        this.add(refreshButton, BorderLayout.NORTH);

        setTitle("Simple OLEP - " + this.sender);
        setSize(900, 500);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
    }
}
