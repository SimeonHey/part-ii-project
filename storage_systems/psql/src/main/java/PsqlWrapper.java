import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PsqlWrapper {
    private Connection connection;

    public PsqlWrapper(Connection connection) {
        this.connection = connection;
    }

    private void insertMessage(String sender, String messageText, Long uuid) throws SQLException {
        connection.setAutoCommit(false);

        String query = String.format("INSERT INTO messages (sender, messageText, uuid) VALUES ('%s', '%s', %d)",
            sender,
            messageText,
            uuid);
        System.out.println(query);

        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();
        connection.commit();
    }

    public void postMessage(PostMessageRequest postMessageRequest, Long uuid) {
        System.out.println("PSQL posts message " + postMessageRequest + " with uuid " + uuid);
        try {
            insertMessage(postMessageRequest.getSender(), postMessageRequest.getMessageText(), uuid);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getMessageDetails(MessageDetailsRequest messageDetailsRequest) {
        System.out.println("Psql has to get details for message " + messageDetailsRequest.getUuid());

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(String.format("SELECT * FROM messages WHERE uuid " +
                "= %d", messageDetailsRequest.getUuid()));
            ResultSet resultSet = preparedStatement.executeQuery();
            StringBuilder sbBig = new StringBuilder();

            int columnCount = resultSet.getMetaData().getColumnCount();

            // TODO: Ugly
            while (resultSet.next()) {
                StringBuilder sbSmall = new StringBuilder();
                for (int i=1; i<=columnCount; i++) {
                    if (i < columnCount) {
                        sbSmall.append(resultSet.getString(i)).append(" ");
                    } else {
                        sbSmall.append(resultSet.getLong(i)).append(" ");
                    }
                }
                sbBig.append(sbSmall.toString()).append(" ");
            }
            preparedStatement.close();

            return sbBig.toString();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
// TODO: Junit