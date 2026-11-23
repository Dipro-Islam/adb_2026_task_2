import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
    // Constant expected by Blocking.java
    public static final int POSTGRESQL = 1;

    // Adjust these credentials to match your local PostgreSQL database
    private static final String HOST = "localhost";
    private static final int PORT = 5432;
    private static final String DATABASE = "postgres"; // e.g., "postgres" or your course DB name
    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres";

    public static Connection getDefaultParameterConnection(int dbms) {
        if (dbms == POSTGRESQL) {
            String url = "jdbc:postgresql://" + HOST + ":" + PORT + "/" + DATABASE;
            try {
                return DriverManager.getConnection(url, USER, PASSWORD);
            } catch (SQLException e) {
                System.err.println("Failed to establish database connection.");
                e.printStackTrace();
                return null;
            }
        }
        throw new IllegalArgumentException("Unsupported DBMS type: " + dbms);
    }
}