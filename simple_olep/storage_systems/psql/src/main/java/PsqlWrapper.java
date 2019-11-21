import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

class PsqlWrapper {
    private static final Logger LOGGER = Logger.getLogger(PsqlWrapper.class.getName());
    
    private final Connection connection;

    PsqlWrapper(Connection connection) {
        this.connection = connection;
    }

    private void insertMessage(String sender, String messageText, Long uuid) throws SQLException {
        connection.setAutoCommit(false);

        String query = String.format("INSERT INTO messages (sender, messageText, uuid) VALUES ('%s', '%s', %d)",
            sender,
            messageText,
            uuid);
        LOGGER.info(query);

        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();
        connection.commit();
    }

    void postMessage(RequestPostMessage requestPostMessage, Long uuid) {
        LOGGER.info("PSQL posts message " + requestPostMessage + " with uuid " + uuid);
        try {
            insertMessage(requestPostMessage.getSender(), requestPostMessage.getMessageText(), uuid);
        } catch (SQLException e) {
            LOGGER.warning("Error when inserting message: " + e);
            throw new RuntimeException(e);
        }
    }

    String getMessageDetails(RequestMessageDetails requestMessageDetails) {
        LOGGER.info("Psql has to get details for message " + requestMessageDetails.getUuid());

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(String.format("SELECT * FROM messages WHERE uuid " +
                "= %d", requestMessageDetails.getUuid()));
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
            LOGGER.warning("SQL exception when doing sql stuff: " + e);
            throw new RuntimeException(e);
        }
    }
}
// TODO: Junit