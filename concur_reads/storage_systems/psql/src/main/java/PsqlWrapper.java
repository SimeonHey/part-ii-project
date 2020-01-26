import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;
import java.util.logging.Logger;

class PsqlWrapper {
    private static final Logger LOGGER = Logger.getLogger(PsqlWrapper.class.getName());
    
    private final Supplier<Connection> connectionSupplier;

    PsqlWrapper(Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    private void insertMessage(String sender, String messageText, Long uuid) throws SQLException {
        String query = String.format("INSERT INTO messages (sender, messageText, uuid) VALUES ($$%s$$, $$%s$$, %d)",
            sender,
            messageText,
            uuid);
        LOGGER.info(query);
        try (Connection connection = connectionSupplier.get()) {
            SqlUtils.executeStatement(query, connection);
        }
    }

    void postMessage(RequestPostMessage requestPostMessage) {
        LOGGER.info("PSQL posts message " + requestPostMessage);
        try {
            insertMessage(requestPostMessage.getMessage().getSender(),
                requestPostMessage.getMessage().getMessageText(), requestPostMessage.getUuid());
        } catch (SQLException e) {
            LOGGER.warning("Error when inserting message: " + e);
            throw new RuntimeException(e);
        }
    }

    ResponseMessageDetails getMessageDetails(RequestMessageDetails requestMessageDetails) {
        try (Connection connection = connectionSupplier.get()) {
            return getMessageDetails(connection, requestMessageDetails);
        } catch (SQLException e) {
            LOGGER.warning("Error when trying to close the new connection");
            throw new RuntimeException(e);
        }
    }

    ResponseMessageDetails getMessageDetails(Connection connection, RequestMessageDetails requestMessageDetails) {
        LOGGER.info("Psql has to get details for message " + requestMessageDetails.getMessageUUID());

        try {
            String statement = String.format("SELECT * FROM messages WHERE uuid = %d",
            requestMessageDetails.getMessageUUID());

            try (ResultSet resultSet = SqlUtils.executeStatementForResult(statement, connection)) {

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

    ResponseAllMessages getAllMessages(Connection connection) {
        LOGGER.info("Psql has to get ALL messages");
        ResponseAllMessages responseAllMessages = new ResponseAllMessages();

        try {
            String statement = "SELECT * FROM messages";
            try (ResultSet resultSet = SqlUtils.executeStatementForResult(statement, connection)) {

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


    ResponseAllMessages getAllMessages() {
        try (Connection connection = connectionSupplier.get()) {
            return getAllMessages(connection);
        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in getAllMessages or when closing the connection: " + e);
            throw new RuntimeException(e);
        }
    }

    void deleteAllMessages() {
        LOGGER.info("Psql is deleting ALL messages");
        try (Connection connection = connectionSupplier.get()) {
            SqlUtils.executeStatement("DELETE FROM messages", connection);
        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in delete all messages: " + e);
            throw new RuntimeException(e);
        }
    }

    Connection newSnapshotIsolatedConnection() {
        Connection connection = connectionSupplier.get();
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
}
