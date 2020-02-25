import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

public class SqlUtils {
    private static final Logger LOGGER = Logger.getLogger(SqlUtils.class.getName());

    public static Connection defaultConnection() {
        return obtainConnection(Constants.PSQL_USER_PASS[0], Constants.PSQL_USER_PASS[1], Constants.PSQL_ADDRESS);
    }

    public static Connection obtainConnection(String username, String password, String psqlAddress) {
        // Initialize Database connection
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        try {
            return DriverManager.getConnection(psqlAddress, props);
        } catch (SQLException e) {
            LOGGER.warning("Error when trying to obtain a connection: " + e);
            throw new RuntimeException(e);
        }
    }


    Connection newSnapshotIsolatedConnection() {
        Connection connection = defaultConnection();

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

    public static void executeStatement(String sql, Connection connection) throws SQLException {
        LOGGER.info("Executing statement: " + sql);
        connection.setAutoCommit(true);
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
    }

    public static ResultSet executeStatementForResult(String sql, Connection connection) throws SQLException {
        LOGGER.info("Executing statement for result: " + sql);
        connection.setAutoCommit(true);
        Statement plainStatement = connection.createStatement();
        return plainStatement.executeQuery(sql);
    }

    public static Message extractMessageFromResultSet(ResultSet resultSet) throws SQLException {
        return new Message(resultSet.getString(1), resultSet.getString(2));
    }

    public static long extractUuidFromResultSet(ResultSet resultSet) throws SQLException {
        return resultSet.getLong(3);
    }

    public static int resultSetSize(ResultSet resultSet) throws SQLException {
        int cnt = 0;
        while (resultSet.next()) {
            cnt ++;
        }
        return cnt;
    }
}
