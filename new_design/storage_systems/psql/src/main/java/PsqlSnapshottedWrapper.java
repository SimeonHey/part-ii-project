import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class PsqlSnapshottedWrapper implements WrappedSnapshottedStorageSystem<Connection> {
    private static final Logger LOGGER = Logger.getLogger(PsqlSnapshottedWrapper.class.getName());

    private static final Connection sequentialConnection = SqlUtils.defaultConnection();

    PsqlSnapshottedWrapper() {

    }

    private void insertMessage(String sender, String messageText, Long uuid) throws SQLException {
        String query = String.format("INSERT INTO messages (sender, messageText, uuid) VALUES ($$%s$$, $$%s$$, %d)",
            sender,
            messageText,
            uuid);

        SqlUtils.executeStatement(query, sequentialConnection);
    }

    @Override
    public ResponseMessageDetails getMessageDetails(Connection snapshot,
                                                    RequestMessageDetails requestMessageDetails) {
        LOGGER.info("Psql has to get details for message " + requestMessageDetails.getMessageUUID());

        try {
            String statement = String.format("SELECT * FROM messages WHERE uuid = %d",
                requestMessageDetails.getMessageUUID());

            try (ResultSet resultSet = SqlUtils.executeStatementForResult(statement, snapshot)) {
                boolean hasMore = resultSet.next();
                if (!hasMore) {
                    return new ResponseMessageDetails(null, requestMessageDetails.getMessageUUID());
                }

                return new ResponseMessageDetails(
                    SqlUtils.extractMessageFromResultSet(resultSet),
                    SqlUtils.extractUuidFromResultSet(resultSet));
            }

        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in getMessageDetails: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResponseAllMessages getAllMessages(Connection snapshot,
                                              RequestAllMessages requestAllMessages) {
        LOGGER.info("Psql has to get ALL messages");
        ResponseAllMessages responseAllMessages = new ResponseAllMessages();

        try {
            String statement = "SELECT * FROM messages";
            try (ResultSet resultSet = SqlUtils.executeStatementForResult(statement, snapshot)) {

                while (resultSet.next()) {
                    responseAllMessages.addMessage(SqlUtils.extractMessageFromResultSet(resultSet));
                }

                LOGGER.info("Psql extracted the following: " + responseAllMessages);

                return responseAllMessages;
            }

        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in getAllMessages: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResponseSearchMessage searchMessage(Connection snapshot,
                                               RequestSearchMessage requestSearchMessage) {
        throw new RuntimeException("PSQL doesn't have search functionality implemented");
    }

    @Override
    public void postMessage(RequestPostMessage requestPostMessage) {
        LOGGER.info("PSQL posts message " + requestPostMessage);
        try {
            insertMessage(requestPostMessage.getMessage().getSender(),
                requestPostMessage.getMessage().getMessageText(), requestPostMessage.getChannelID());
        } catch (SQLException e) {
            LOGGER.warning("Error when inserting message: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteAllMessages() {
        LOGGER.info("Psql is deleting ALL messages");
        try {
            SqlUtils.executeStatement("DELETE FROM messages", sequentialConnection);
        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in delete all messages: " + e);
            throw new RuntimeException(e);
        }
    }

    public Connection getDefaultSnapshot() {
        return sequentialConnection;
    }

    public Connection getConcurrentSnapshot() {
        Connection connection = SqlUtils.defaultConnection();

        try {
            connection.setAutoCommit(false);
            SqlUtils.executeStatement("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", connection);

            // Force the transaction to get a txid, so that a snapshot of the data is saved
            try (ResultSet resultSet = SqlUtils.executeStatementForResult("SELECT txid_current()", connection)) {
                resultSet.next();
                long txId = resultSet.getLong(1);
                LOGGER.info("Started a transaction with txid " + txId);
            } catch (Exception e) {
                LOGGER.warning("Error when tried to read the transaction id: " + e);
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            LOGGER.warning("Error when trying to open a new transaction: " + e);
            throw new RuntimeException(e);
        }

        return connection;
    }

    @Override
    public void close() throws Exception {

    }
}
