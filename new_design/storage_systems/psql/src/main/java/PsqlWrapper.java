import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * A wrapper around PSQL that uses the same connection for all requests.
 * Hence, it offers no instantaneous snapshots of the data.
 */
class PsqlWrapper extends PsqlSnapshottedWrapper implements WrappedStorageSystem {
    private static final Logger LOGGER = Logger.getLogger(PsqlWrapper.class.getName());

    PsqlWrapper(Connection connectionToUse) {
        super(() -> connectionToUse);
    }

    // Read requests
    @Override
    public ResponseMessageDetails getMessageDetails(RequestMessageDetails requestMessageDetails) {
        try (Connection connection = super.connectionSupplier.get()) {
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


    @Override
    public ResponseAllMessages getAllMessages(RequestAllMessages allMessages) {
        try (Connection connection = super.connectionSupplier.get()) {
            return getAllMessages(connection);
        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in getAllMessages or when closing the connection: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResponseSearchMessage searchMessage(RequestSearchMessage requestSearchMessage) {
        throw new RuntimeException("PSQL doesn't have search functionality implemented");
    }

    // Write requests
    @Override
    public void postMessage(RequestPostMessage requestPostMessage) {
        LOGGER.info("PSQL posts message " + requestPostMessage);
        try {
            super.insertMessage(requestPostMessage.getMessage().getSender(),
                requestPostMessage.getMessage().getMessageText(), requestPostMessage.getRequestUUID());
        } catch (SQLException e) {
            LOGGER.warning("Error when inserting message: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteAllMessages() {
        LOGGER.info("Psql is deleting ALL messages");
        try (Connection connection = super.connectionSupplier.get()) {
            SqlUtils.executeStatement("DELETE FROM messages", connection);
        } catch (SQLException e) {
            LOGGER.warning("SQL exception when doing sql stuff in delete all messages: " + e);
            throw new RuntimeException(e);
        }
    }

    /*
    // Make sure those methods are not used, as they don't belong here (TODO: Fix that)
    @Deprecated
    @Override
    public ResponseMessageDetails getMessageDetails(SnapshotHolder<Connection> snapshotHolder,
                                                    RequestMessageDetails requestMessageDetails) {
        throw new RuntimeException("Not implemented");
    }

    @Deprecated
    @Override
    public ResponseAllMessages getAllMessages(SnapshotHolder<Connection> snapshotHolder,
                                              RequestAllMessages requestAllMessages) {
        throw new RuntimeException("Not implemented");
    }

    @Deprecated
    @Override
    public ResponseSearchMessage searchMessage(SnapshotHolder<Connection> snapshotHolder,
                                               RequestSearchMessage requestSearchMessage) {
        throw new RuntimeException("Not implemented");
    }

    @Deprecated
    @Override
    Connection newSnapshotIsolatedConnection() {
        throw new RuntimeException("Not implemented");
    }
    */
}
