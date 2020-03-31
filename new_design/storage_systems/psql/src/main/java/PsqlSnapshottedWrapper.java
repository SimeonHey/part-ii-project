
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class PsqlSnapshottedWrapper implements WrappedSnapshottedStorageSystem<WrappedConnection> {
    private static final Logger LOGGER = Logger.getLogger(PsqlSnapshottedWrapper.class.getName());
    public static final int MAX_OPENED_CONNECTIONS = 90;

    private final WrappedConnection sequentialConnection;

    private final Semaphore connectionsSemaphore = new Semaphore(MAX_OPENED_CONNECTIONS);
    private final BlockingQueue<WrappedConnection> concurrentConectionsPool =
        new LinkedBlockingDeque<>(MAX_OPENED_CONNECTIONS);

    PsqlSnapshottedWrapper() {
        sequentialConnection = new WrappedConnection(SqlUtils.freshDefaultConnection(), (c) -> {}, -1);
    }

    private void insertMessage(String sender, String messageText, Long uuid) throws SQLException {
        String query = String.format("INSERT INTO messages (sender, messageText, uuid) VALUES ($$%s$$, $$%s$$, %d)",
            sender,
            messageText,
            uuid);

        SqlUtils.executeStatement(query, sequentialConnection.getConnection());
    }

    @Override
    public ResponseMessageDetails getMessageDetails(WrappedConnection wrappedConnection,
                                                    RequestMessageDetails requestMessageDetails) {
        LOGGER.info("Psql has to get details for message " + requestMessageDetails.getMessageUUID());

        try {
            String statement = String.format("SELECT * FROM messages WHERE uuid = %d",
                requestMessageDetails.getMessageUUID());

            try (ResultSet resultSet = SqlUtils.executeStatementForResult(statement, wrappedConnection.getConnection())) {
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
    public ResponseAllMessages getAllMessages(WrappedConnection wrappedConnection,
                                              RequestAllMessages requestAllMessages) {
        /*
        try {
            Thread.sleep(200); // TODO: Remove
        } catch (InterruptedException e) {
            LOGGER.warning("Error while waiting");
            throw new RuntimeException(e);
        }*/

        LOGGER.info("Psql has to get ALL messages");
        ResponseAllMessages responseAllMessages = new ResponseAllMessages();

        try {
            String statement = "SELECT * FROM messages";
            try (ResultSet resultSet = SqlUtils.executeStatementForResult(statement, wrappedConnection.getConnection())) {

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
    public ResponseSearchMessage searchMessage(WrappedConnection wrappedConnection,
                                               RequestSearchMessage requestSearchMessage) {
        throw new RuntimeException("PSQL doesn't have search functionality implemented");
    }

    @Override
    public void postMessage(RequestPostMessage requestPostMessage) {
        LOGGER.info("PSQL posts message " + requestPostMessage);
        try {
            insertMessage(
                requestPostMessage.getMessage().getSender(),
                requestPostMessage.getMessage().getMessageText(),
                requestPostMessage.getResponseAddress().getChannelID());
        } catch (SQLException e) {
            LOGGER.warning("Error when inserting message: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteAllMessages() {
        LOGGER.info("Psql is deleting ALL messages");
        try {
            SqlUtils.executeStatement("DELETE FROM messages", sequentialConnection.getConnection());
        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in delete all messages: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getMaxNumberOfSnapshots() {
        return 50;
    }

    public WrappedConnection getDefaultSnapshot() {
        return sequentialConnection;
    }

    private void concurrentConnectionClosedCallback(WrappedConnection wrappedConnection) {
        LOGGER.info("Putting connection with txId " + wrappedConnection.getTxId() + " back into the pool...");

        try {
            wrappedConnection.getConnection().commit();
        } catch (SQLException e) {
            // TODO: Maybe we don't care about the outcome here..
            LOGGER.warning("Error when trying to commit old concurrent connection: " + e);
            throw new RuntimeException(e);
        }

        try {
            concurrentConectionsPool.put(wrappedConnection);
        } catch (InterruptedException e) {
            LOGGER.warning("Error when trying to put a wrapped connection back in the pool to be reused: " + e);
            throw new RuntimeException(e);
        }
    }

    private WrappedConnection freshConcurrentConnection() {
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

        return new WrappedConnection(connection, this::concurrentConnectionClosedCallback, txId);
    }

    @Override
    public WrappedConnection getConcurrentSnapshot() {
        // First, try and re-use one from the poll, with no blocking!
        WrappedConnection pooled = concurrentConectionsPool.poll(); // TODO: Might want to wait for a while :)
        if (pooled != null) {
            LOGGER.info("Returning a pooled connection");
            return pooled;
        }

        // If such is not available, try and create a new one, no blocking again (because nothing is releasing
        // resources)
        boolean canAcquire = connectionsSemaphore.tryAcquire();
        if (canAcquire) {
            LOGGER.info("Returning a fresh connection");
            return freshConcurrentConnection();
        }

        // If we can't create a new one because the limit has been reached, block until a pooled one is available
        try {
            LOGGER.info("Waiting for a pooled connection and returning it...");
            return concurrentConectionsPool.take();
        } catch (InterruptedException e) {
            LOGGER.warning("Error when waiting on a pooled connection to become available: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
