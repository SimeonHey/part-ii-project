import java.sql.*;
import java.util.Properties;

public class EntryPoint {

    public static void addMessage(Connection dbConnection, int from_id, int to_id, String message) throws SQLException {
        dbConnection.setAutoCommit(false);

        String query = String.format("INSERT INTO messages (from_id, to_id, message) VALUES (%d, %d, '%s')", from_id,
            to_id, message);

        PreparedStatement preparedStatement = dbConnection.prepareStatement(query);
        preparedStatement.executeUpdate();
        dbConnection.commit();
    }

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:postgresql://localhost/termtest";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "default");

        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM messages");
        ResultSet resultSet = preparedStatement.executeQuery();

        int columnCount = resultSet.getMetaData().getColumnCount();
        int currentRow = 0;
        while (resultSet.next()) {
            currentRow++;
            System.out.println("Row " + currentRow);
            for (int i=1; i<=columnCount; i++) {
                System.out.print(resultSet.getString(i) + " ");
            }
            System.out.println();
        }
        preparedStatement.close();

        addMessage(conn, 1, 2, "Cool!");
    }
}
