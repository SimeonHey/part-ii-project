
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class PsqlSnapshottedSystem extends SnapshottedDatabase<Connection>
    implements MessageAppDatabase<Connection> {

    private static final Logger LOGGER = Logger.getLogger(PsqlSnapshottedSystem.class.getName());
    public static final int MAX_OPENED_CONNECTIONS = 90;

    private final Connection sequentialConnection;

    PsqlSnapshottedSystem() {
        super(MAX_OPENED_CONNECTIONS);
        sequentialConnection = SqlUtils.freshDefaultConnection();
    }

    private void insertMessage(Message message, Long messageId) throws SQLException {
        String query = String.format("INSERT INTO messages (sender, recipient, messagetext, timesent, messageid) " +
                "VALUES " + "($$%s$$, $$%s$$, $$%s$$, %d, %d)",
            message.getSender(),
            message.getRecipient(),
            message.getMessageText(),
            message.getTimestamp(),
            messageId);

        SqlUtils.executeStatement(query, sequentialConnection);
    }

    @Override
    public ResponseMessageDetails getMessageDetails(Connection connection,
                                                    RequestMessageDetails requestMessageDetails) {
        LOGGER.info("Psql has to get details for message " + requestMessageDetails.getMessageID());

        try {
            String statement = String.format("SELECT * FROM messages WHERE messageid = %d",
                requestMessageDetails.getMessageID());

            try (ResultSet resultSet = SqlUtils.executeStatementForResult(statement, connection)) {
                boolean hasMore = resultSet.next();
                if (!hasMore) {
                    LOGGER.info("Psql got no results for message with messageid = " +
                        requestMessageDetails.getMessageID());
                    return new ResponseMessageDetails(null, requestMessageDetails.getMessageID());
                }

                LOGGER.info("Got a result for message with messageid = " +
                    requestMessageDetails.getMessageID());
                return new ResponseMessageDetails(
                    SqlUtils.extractMessageFromResultSet(resultSet),
                    SqlUtils.extractIDFromResultSet(resultSet));
            }

        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in getMessageDetails: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResponseAllMessages getConvoMessages(Connection connection,
                                                RequestConvoMessages requestConvoMessages) {
        LOGGER.info("Psql has to get ALL messages");
        ResponseAllMessages responseAllMessages = new ResponseAllMessages();

        try {
            String statement = String.format("SELECT * FROM messages WHERE " +
                "(sender=$$%s$$ AND recipient=$$%s$$) OR (sender=$$%s$$ AND recipient=$$%s$$)",
                requestConvoMessages.getRequester(), requestConvoMessages.getWithUser(),
                requestConvoMessages.getWithUser(), requestConvoMessages.getRequester());

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

    @Override
    public ResponseSearchMessage searchMessage(Connection connection,
                                               RequestSearchMessage requestSearchMessage) {
        throw new RuntimeException("PSQL doesn't have search functionality implemented");
    }

    @Override
    public void postMessage(RequestPostMessage requestPostMessage) {
        LOGGER.info("PSQL posts message " + requestPostMessage);
        try {
            insertMessage(requestPostMessage.getMessage(), requestPostMessage.getResponseAddress().getChannelID());
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

    @Override
    public void deleteConvoThread(RequestDeleteConversation deleteConversation) {
        LOGGER.info("Psql is deleting ALL messages");
        try {
            SqlUtils.executeStatement(String.format("DELETE FROM messages " +
                "WHERE (sender=$$%s$$ AND recipient=$$%s$$) OR (sender=$$%s$$ AND recipient=$$%s$$)",
                deleteConversation.getUser1(), deleteConversation.getUser2(),
                deleteConversation.getUser2(), deleteConversation.getUser1()), sequentialConnection);
        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in delete all messages: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Connection getMainDataView() {
        return sequentialConnection;
    }

    @Override
    Connection freshConcurrentSnapshot() {
        final Connection connection = SqlUtils.freshDefaultConnection();
        final long txId;

        try {
            connection.setAutoCommit(false);
            SqlUtils.executeStatement("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", connection);

            // Force the transaction to get a txid, so that a snapshot of the data is saved
            try (ResultSet resultSet = SqlUtils.executeStatementForResult("SELECT txid_current()", connection)) {

                resultSet.next();
                txId = resultSet.getLong(1);
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
    Connection refreshSnapshot(Connection bareSnapshot) {
        try {
            bareSnapshot.commit();
        } catch (SQLException e) {
            LOGGER.warning("Error when tryin to refresh a concurren connection by commiting it: " + e);
            throw new RuntimeException(e);
        }

        return bareSnapshot;
    }

    @Override
    public void close() throws Exception {

    }
}
