import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class SearchPopup extends JFrame {
    private static final Logger LOGGER = Logger.getLogger(SearchPopup.class.getName());

    private String formatResponse(StorageAPI storageAPI,
                                  ResponseSearchMessage responseSearchMessage) {
        List<Message> messageList = responseSearchMessage.getOccurrences().stream().map(occurrence -> {
            try {
                return storageAPI.messageDetails(occurrence).getMessage();
            } catch (IOException e) {
                LOGGER.warning("Error when getting search results details");
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        return messageList.toString();
    }

    SearchPopup(StorageAPI storageAPI,
                ResponseSearchMessage responseSearchMessage) {
        LOGGER.info("Started popup with search message response: " + responseSearchMessage);

        this.setLayout(new BorderLayout());

        JTextArea messageDisplay = new JTextArea();
        messageDisplay.setEditable(false);

        messageDisplay.setText(formatResponse(storageAPI, responseSearchMessage));

        JScrollPane scrollMessageDisplay = new JScrollPane(messageDisplay);
        this.add(scrollMessageDisplay, BorderLayout.CENTER);

        setTitle("Search results");
        setSize(400, 200);
        setLocationRelativeTo(null);
//        setDefaultCloseOperation(EXIT_ON_CLOSE);

        this.setVisible(true);
    }
}
