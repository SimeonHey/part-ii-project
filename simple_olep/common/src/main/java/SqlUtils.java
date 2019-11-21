import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class SqlUtils {
    private static final Logger LOGGER = Logger.getLogger(SqlUtils.class.getName());

    public static void executeStatement(String sql, Connection connection) throws SQLException {
        LOGGER.info("Executing statement: " + sql);
        connection.setAutoCommit(false);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.executeUpdate();
        connection.commit();
    }
    public static ResultSet executeStatementForResult(String sql, Connection connection) throws SQLException {
        LOGGER.info("Executing statement for result: " + sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        return preparedStatement.executeQuery();
    }
}
