import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

public class SqlUtils {
    private static final Logger LOGGER = Logger.getLogger(SqlUtils.class.getName());

    public static Connection freshDefaultConnection() {
        return obtainConnection(ConstantsMAPP.PSQL_USER_PASS[0], ConstantsMAPP.PSQL_USER_PASS[1], ConstantsMAPP.PSQL_ADDRESS);
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

    public static void executeStatement(String sql, Connection connection) throws SQLException {
        LOGGER.info("Executing SQL statement of length " + sql.length());
//        connection.setAutoCommit(true);
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
    }

    public static ResultSet executeStatementForResult(String sql, Connection connection) throws SQLException {
        LOGGER.info("Executing SQL statement of length " + sql.length());
//        connection.setAutoCommit(true);
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
