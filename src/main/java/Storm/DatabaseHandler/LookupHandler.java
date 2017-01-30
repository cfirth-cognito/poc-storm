package Storm.DatabaseHandler;


import java.sql.*;

/**
 * Created by Charlie on 28/01/2017.
 */
public class LookupHandler {

    // hard coded config for now
    static String url = "jdbc:mysql://localhost:3306/hermes_mi";
    static String user = "root";
    static String pass = "root";
    static String prepareStatement = "SELECT id FROM (tbl) WHERE (col) = ?";

    static Connection connection;
    static PreparedStatement stmt;

    // Throw any exceptions we encounter. This ensure's the bolt worker is killed, and another bolt spun up to try again.
    static int lookupId(String table, String column, String value) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        if (connection == null) {
            connection = DriverManager.getConnection(url, user, pass);
        }
        prepareStatement = prepareStatement.replace("(tbl)", table);
        prepareStatement = prepareStatement.replace("(col)", column);
        stmt = connection.prepareStatement(prepareStatement);

        System.out.println(String.format("CF Looking up from %s, column %s, value %s", table, column, value));
        stmt.setString(1, value);

        ResultSet resultSet = stmt.executeQuery();

        if (resultSet.next()) {
            return resultSet.getInt(1);
        }

        return 1; // Unknown
    }


}
