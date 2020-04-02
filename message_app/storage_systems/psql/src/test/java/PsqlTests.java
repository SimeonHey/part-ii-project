import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PsqlTests {
    private final static Logger LOGGER = Logger.getLogger(PsqlTests.class.getName());

    @Test
    public void testNewTransactionConnectionIsSnapshotIsolated() throws SQLException {
        // We will just test the .newTransactionConnection() method
        PsqlSnapshottedWrapper psqlWrapper = new PsqlSnapshottedWrapper();

        // Initialize a normal, always-commiting transaction
        Connection normalConnection = SqlUtils.obtainConnection(ConstantsMAPP.PSQL_USER_PASS[0],
            ConstantsMAPP.PSQL_USER_PASS[1], ConstantsMAPP.PSQL_ADDRESS);
        normalConnection.setAutoCommit(true);

        // Prepare the tables
        SqlUtils.executeStatement("DELETE FROM test", normalConnection);
        SqlUtils.executeStatement("DELETE FROM test2", normalConnection);
        SqlUtils.executeStatement("INSERT INTO test2 VALUES ($$hello$$)", normalConnection);
        // TODO: Tables might not be there on a fresh run
//        SqlUtils.executeStatement("CREATE TABLE test (col1 text)", normalConnection);
//        SqlUtils.executeStatement("CREATE TABLE test2 (col1 text)", normalConnection);

        // Initialize a connection which has its own transaction
        Connection transactionConnection = psqlWrapper.getConcurrentSnapshot().getSnapshot();

        // Insert things from the normal connection.
        SqlUtils.executeStatement("INSERT INTO test VALUES ($$hello$$)", normalConnection);

        // The transaction connection shouldn't be able to see them
        ResultSet resultSet;

        resultSet = SqlUtils.executeStatementForResult("SELECT * FROM test", transactionConnection);
        assertEquals(0, SqlUtils.resultSetSize(resultSet));

        resultSet = SqlUtils.executeStatementForResult("SELECT * FROM test2", transactionConnection);
        assertEquals(1, SqlUtils.resultSetSize(resultSet));
    }

    @Test
    public void testConnectionPooling() throws Exception {
        PsqlSnapshottedWrapper psqlSnapshottedWrapper = new PsqlSnapshottedWrapper();
        List<SnapshotHolder> openedConnections = new ArrayList<>();

        for (int i=0; i<PsqlSnapshottedWrapper.MAX_OPENED_CONNECTIONS; i++) {
            var current = psqlSnapshottedWrapper.getConcurrentSnapshot();
            openedConnections.add(current);
            LOGGER.info("Added a connectinon " + current);
        }

        CompletableFuture<SnapshotHolder> oneMore =
            CompletableFuture.supplyAsync(psqlSnapshottedWrapper::getConcurrentSnapshot);

        Thread.sleep(1000);

        assertFalse(oneMore.isDone());

        openedConnections.get(0).close();

        Thread.sleep(1000);
        assertTrue(oneMore.isDone());
        assertEquals(openedConnections.get(0).getSnapshotId(), oneMore.get().getSnapshotId());
    }
}
