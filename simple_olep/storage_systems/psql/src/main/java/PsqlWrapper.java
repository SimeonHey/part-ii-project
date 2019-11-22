import java.sql.Connection;
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
        String query = String.format("INSERT INTO messages (sender, messageText, uuid) VALUES ('%s', '%s', %d)",
            sender,
            messageText,
            uuid);
        LOGGER.info(query);
        SqlUtils.executeStatement(query, this.connection);
    }

    void postMessage(RequestPostMessage requestPostMessage, Long uuid) {
        LOGGER.info("PSQL posts message " + requestPostMessage + " with uuid " + uuid);
        try {
            insertMessage(requestPostMessage.getMessage().getSender(),
                requestPostMessage.getMessage().getMessageText(), uuid);
        } catch (SQLException e) {
            LOGGER.warning("Error when inserting message: " + e);
            throw new RuntimeException(e);
        }
    }

    ResponseMessageDetails getMessageDetails(RequestMessageDetails requestMessageDetails) {
        LOGGER.info("Psql has to get details for message " + requestMessageDetails.getUuid());

        try {
            String statement = String.format("SELECT * FROM messages WHERE uuid = %d",
                requestMessageDetails.getUuid());
            ResultSet resultSet = SqlUtils.executeStatementForResult(statement, this.connection);

            boolean hasMore = resultSet.next();
            if (!hasMore) {
                return null;
            }

            ResponseMessageDetails response = new ResponseMessageDetails(
                new Message(resultSet.getString(1), resultSet.getString(2)),
                resultSet.getLong(3));
            resultSet.close();

            return response;

        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff: " + e);
            throw new RuntimeException(e);
        }
    }
}
// TODO: Junit