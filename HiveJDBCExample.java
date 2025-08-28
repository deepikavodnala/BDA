import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;

public class HiveJDBCExample {
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL    = "jdbc:hive2://localhost:10000/default";
    private static final String USER   = "cloudera"; // adjust if needed
    private static final String PASS   = "";         // blank is fine on QuickStart

    public static void main(String[] args) {
        try (Connection con = connect(); Statement stmt = con.createStatement()) {

            // Create and use a database
            stmt.execute("CREATE DATABASE IF NOT EXISTS employee_db");
            stmt.execute("USE employee_db");

            // Drop + create table
            stmt.execute("DROP TABLE IF EXISTS employee");
            stmt.execute(
                "CREATE TABLE employee (" +
                "  empname       STRING, " +
                "  salary        DOUBLE, " +   // use DOUBLE for money-like numeric
                "  age           INT, " +
                "  joining_date  DATE, " +
                "  address       STRING" +
                ") " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
                "STORED AS TEXTFILE"
            );

            System.out.println("? Created database employee_db and table employee.");
        } catch (SQLException e) {
            System.err.println("SQL error: " + e.getMessage());
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.err.println("Driver not found: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Connection connect() throws SQLException, ClassNotFoundException {
        Class.forName(DRIVER);
        // If your cluster requires auth, change USER/PASS or add URL params.
        return DriverManager.getConnection(URL, USER, PASS);
    }
}
