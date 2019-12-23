import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

class FrontEnd extends JFrame {
    private static final Logger LOGGER = Logger.getLogger(FrontEnd.class.getName());
    private StorageAPI storageAPI;

    private String sender;

    private JTextArea messageDisplay;

    private TextField messageField;

    private TextField searchField;

    FrontEnd(String sender, StorageAPI storageAPI) {
        this.sender = sender;
        this.storageAPI = storageAPI;

        initUI();
    }

    private void addMessage(Message message) {
        String currentText = this.messageDisplay.getText();
        String newText = currentText +
            String.format("\n%s said: %s", message.getSender(), message.getMessageText());
        this.messageDisplay.setText(newText);
    }

    private void setMessages(List<Message> messages) {
        this.messageDisplay.setText("");
        messages.forEach(this::addMessage);
    }

    private void sendCurrentMessage() {
        LOGGER.info("Sending message was triggered, and the text is " +
            messageField.getText());

        Message message = new Message(this.sender, messageField.getText());
        this.storageAPI.postMessage(message);
        this.messageField.setText("");
    }

    private void searchCurrentToken() {
        String token = searchField.getText();
        LOGGER.info("Searching was triggered, and the token is " + token);

        ResponseSearchMessage response;
        try {
            response = storageAPI.searchMessage(token);
        } catch (IOException e) {
            LOGGER.warning("Error when searching for message: " + e);
            throw new RuntimeException(e);
        }
        LOGGER.info("Search completed. Occurrences: " + response.getOccurrences());

        EventQueue.invokeLater(() -> new SearchPopup(storageAPI, response));
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

    private JPanel initSearchControls() {
        JPanel jPanel = new JPanel();
        jPanel.setLayout(new BorderLayout());

        this.searchField = new TextField();
        this.searchField.addActionListener(e -> searchCurrentToken());

        jPanel.add(searchField, BorderLayout.CENTER);

        var searchButton = new JButton("Search for token");
        searchButton.addActionListener((event) -> searchCurrentToken());
        jPanel.add(searchButton, BorderLayout.EAST);

        return jPanel;
    }

    private JPanel initBottomControls() {
        JPanel jPanel = new JPanel();
        jPanel.setLayout(new BorderLayout());

        jPanel.add(initMessageControls(), BorderLayout.NORTH);
        jPanel.add(initSearchControls(), BorderLayout.SOUTH);

        return jPanel;
    }

    private void initUI() {
        this.setLayout(new BorderLayout());

        this.messageDisplay = new JTextArea();
        messageDisplay.setEditable(false);

        JScrollPane scrollMessageDisplay = new JScrollPane(this.messageDisplay);
        this.add(scrollMessageDisplay, BorderLayout.CENTER);

        JPanel bottomControls = initBottomControls();
        this.add(bottomControls, BorderLayout.SOUTH);

        JButton refreshButton = new JButton("Refresh thread");
        refreshButton.addActionListener(e -> {
            try {
                setMessages(storageAPI.allMessages().getMessages());
            } catch (IOException ex) {
                LOGGER.warning("Error when getting all messages: " + e);
                throw new RuntimeException(ex);
            }
        });
        this.add(refreshButton, BorderLayout.NORTH);

        setTitle("Simple OLEP - " + this.sender);
        setSize(900, 500);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(EXIT_ON_CLOSE);

        this.setVisible(true);
    }
}