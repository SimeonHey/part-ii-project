import java.sql.Connection;
import java.util.concurrent.Semaphore;

public class WrappedConnection implements AutoCloseable {
    private final Connection connection;
    private final Semaphore semaphoreConnections;

    public WrappedConnection(Connection connection, Semaphore semaphoreConnections) {
        this.connection = connection;
        this.semaphoreConnections = semaphoreConnections;
    }

    public Connection getConnection() {
        return connection;
    }

    public Semaphore getSemaphoreConnections() {
        return semaphoreConnections;
    }

    @Override
    public void close() throws Exception {
        this.connection.close();
        this.semaphoreConnections.release();
    }
}
