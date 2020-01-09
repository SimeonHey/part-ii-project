import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;

import static org.junit.Assert.assertFalse;

public class PsqlTests {

    @Test
    public void testNewTransactionConnectionIsSnapshotIsolated() throws SQLException {
        Supplier<Connection> connectionSupplier = () -> SqlUtils.obtainConnection(Constants.PSQL_USER_PASS[0],
            Constants.PSQL_USER_PASS[1], Constants.PSQL_ADDRESS);

        // We will just test the .newTransactionConnection() method
        PsqlWrapper psqlWrapper = new PsqlWrapper(connectionSupplier);

        // Initialize a normal, always-commiting transaction
        Connection normalConnection = connectionSupplier.get();

        // Prepare the tables
        SqlUtils.executeStatement("DELETE FROM test", normalConnection);
        SqlUtils.executeStatement("DELETE FROM test2", normalConnection);
//        SqlUtils.executeStatement("CREATE TABLE test (col1 text)", normalConnection);
//        SqlUtils.executeStatement("CREATE TABLE test2 (col1 text)", normalConnection);

        // Initialize a connection which has its own transaction
        Connection transactionConnection = psqlWrapper.newTransactionConnection();

        // Insert things from the normal connection.
        SqlUtils.executeStatement("INSERT INTO test VALUES ($$hello$$)", normalConnection);
        SqlUtils.executeStatement("INSERT INTO test2 VALUES ($$hello$$)", normalConnection);

        // The transaction connection shouldn't be able to see them
        ResultSet resultSet = SqlUtils.executeStatementForResult("SELECT * FROM test", transactionConnection);
        boolean hasAnything = resultSet.next();
        assertFalse(hasAnything);

        resultSet = SqlUtils.executeStatementForResult("SELECT * FROM test2", transactionConnection);
        hasAnything = resultSet.next();
        assertFalse(hasAnything);
    }
}
