import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;

public class WrappedConnection implements AutoCloseable {
    private final Connection connection;
    private final Consumer<WrappedConnection> closeCallback;
    private final long txId;

    public WrappedConnection(Connection connection, Consumer<WrappedConnection> closeCallback, long txId) {
        this.connection = connection;
        this.closeCallback = closeCallback;
        this.txId = txId;
    }

    public Connection getConnection() {
        return connection;
    }

    public long getTxId() {
        return txId;
    }

    @Override
    public String toString() {
        return "WrappedConnection{" +
            "connection=" + connection +
            ", txId=" + txId +
            '}';
    }

    @Override
    public void close() throws Exception {
        closeCallback.accept(this);
    }
}
