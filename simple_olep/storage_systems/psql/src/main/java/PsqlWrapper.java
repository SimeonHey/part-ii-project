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
                SqlUtils.extractMessageFromResultSet(resultSet),
                SqlUtils.extractUuidFromResultSet(resultSet));

            resultSet.close();

            return response;

        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in getMessageDetails: " + e);
            throw new RuntimeException(e);
        }
    }

    ResponseAllMessages getAllMessages(RequestAllMessages requestAllMessages) {
        LOGGER.info("Psql has to get ALL messages: " + requestAllMessages);
        ResponseAllMessages responseAllMessages = new ResponseAllMessages();

        try {
            String statement = "SELECT * FROM messages";
            ResultSet resultSet = SqlUtils.executeStatementForResult(statement, this.connection);

            while (resultSet.next()) {
                responseAllMessages.addMessage(SqlUtils.extractMessageFromResultSet(resultSet));
            }

            LOGGER.info("Psql extracted the following: " + responseAllMessages);

            resultSet.close();
            return responseAllMessages;
        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in getAllMessages: " + e);
            throw new RuntimeException(e);
        }
    }

    void deleteAllMessages() {
        LOGGER.info("Psql is deleting ALL messages");
        try {
            SqlUtils.executeStatement("DELETE FROM messages", this.connection);
        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in delete all messages: " + e);
            throw new RuntimeException(e);
        }
    }
}
// TODO: Junit