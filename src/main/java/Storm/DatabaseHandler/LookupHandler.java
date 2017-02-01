package Storm.DatabaseHandler;


import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */
public class LookupHandler {

    // hard coded config for now
    static String url = "jdbc:mysql://localhost:3306/hermes_mi";
    static String user = "root";
    static String pass = "root";
    static String idLookupStatement = "SELECT id FROM (tbl) WHERE (col) = ?";
    static String dimensionLookupStatement = "SELECT (cols) FROM (tbl) WHERE id = ?";

    static Connection connection;
    static PreparedStatement stmt;

    // Throw any exceptions we encounter. This ensure's the bolt worker is killed, and another bolt spun up to try again.
    static int lookupId(String table, String column, String value) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        if (connection == null) {
            connection = DriverManager.getConnection(url, user, pass);
        }
        idLookupStatement = idLookupStatement.replace("(tbl)", table).replace("(col)", column);
        try {
            stmt = connection.prepareStatement(idLookupStatement);

            System.out.println(String.format("[LOG] Looking up from %s, column %s, value %s", table, column, value));
            stmt.setString(1, value);
            System.out.println("[LOG] Lookup Query: " + idLookupStatement);

            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException | NullPointerException e) {
            System.out.println(String.format("[LOG] Caught Exception %s looking up id in table %s, column %s, value %s. Returning 1.",
                    e.getMessage(), table, column, value));
        }
        return 1; // Unknown
    }

    static List<Object> lookupDimension(String table, Map<String, String> columnsToReturn, String id) throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        List<Object> data = new ArrayList<>();
        System.out.println(String.format("[LOG] Looking up from %s, columns %s, id %s", table, Arrays.toString(columnsToReturn.keySet().toArray()), id));

        if (connection == null) {
            connection = DriverManager.getConnection(url, user, pass);
        }

        dimensionLookupStatement = dimensionLookupStatement.replace("(tbl)", table);
        dimensionLookupStatement = dimensionLookupStatement.replace("(cols)", Arrays.toString(columnsToReturn.keySet().toArray()));
        try {
            stmt = connection.prepareStatement(dimensionLookupStatement);
            stmt.setString(1, id);
            System.out.println("[LOG] Lookup Query: " + dimensionLookupStatement);

            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                for (Map.Entry column : columnsToReturn.entrySet()) {
                    switch (column.getValue().toString()) {
                        case "String":
                            data.add(resultSet.getString(column.getKey().toString()));
                            break;
                        case "Integer":
                            data.add(resultSet.getInt(column.getKey().toString()));
                            break;
                    }
                }

            }
            return data;
        } catch (SQLException | NullPointerException e) {
            System.out.println(String.format("[LOG] Caught Exception %s looking up id in table %s, column %s, value %s. Returning 1.",
                    e.getMessage(), table, Arrays.toString(columnsToReturn.keySet().toArray()), id));
        }
        return null; // Unknown
    }


}
