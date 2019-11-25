import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class SqlUtils {
    private static final Logger LOGGER = Logger.getLogger(SqlUtils.class.getName());

    public static void executeStatement(String sql, Connection connection) throws SQLException {
        LOGGER.info("Executing statement: " + sql);
        connection.setAutoCommit(true);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.executeUpdate();
    }
    public static ResultSet executeStatementForResult(String sql, Connection connection) throws SQLException {
        LOGGER.info("Executing statement for result: " + sql);
        connection.setAutoCommit(true);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        return preparedStatement.executeQuery();
    }

    public static Message extractMessageFromResultSet(ResultSet resultSet) throws SQLException {
        return new Message(resultSet.getString(1), resultSet.getString(2));
    }

    public static long extractUuidFromResultSet(ResultSet resultSet) throws SQLException {
        return resultSet.getLong(3);
    }
}
