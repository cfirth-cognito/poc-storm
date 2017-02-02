package Storm.DatabaseHandler;


import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */
class LookupHandler {

    // hard coded config for now
    private static String url = "jdbc:mysql://localhost:3306/hermes_mi";
    private static String user = "root";
    private static String pass = "root";

    private static Connection connection;
    private static PreparedStatement stmt;

    // Throw any exceptions we encounter. This ensure's the bolt worker is killed, and another bolt spun up to try again.
    static int lookupId(String table, String column, String value) throws ClassNotFoundException, SQLException {
        String idLookupStatement = "SELECT id FROM (tbl) WHERE (col) = ?";
        Class.forName("com.mysql.jdbc.Driver");
        if (connection == null) {
            connection = DriverManager.getConnection(url, user, pass);
        }
        idLookupStatement = idLookupStatement.replace("(tbl)", table).replace("(col)", column);
        try {
            stmt = connection.prepareStatement(idLookupStatement);

//            System.out.println(String.format("[LOG] Looking up from %s, column %s, value %s", table, column, value));
            stmt.setString(1, value);

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

    static int lookupId(String table, List<String> columns, List<String> values) throws ClassNotFoundException, SQLException {
        String idLookupStatement = "SELECT id FROM (tbl) WHERE";
        Class.forName("com.mysql.jdbc.Driver");
        if (connection == null) {
            connection = DriverManager.getConnection(url, user, pass);
        }
        idLookupStatement = idLookupStatement.replace("(tbl)", table);
        for (String column : columns) {
            idLookupStatement += " " + column + " = " + "? AND";
        }
        idLookupStatement = idLookupStatement.substring(0, idLookupStatement.length() - 4);

        try {
            stmt = connection.prepareStatement(idLookupStatement);

            for (String value : values)
                stmt.setString(values.indexOf(value) + 1, value);

            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException | NullPointerException e) {
            System.out.println(String.format("[LOG] Caught Exception %s looking up id in table %s, columns %s, values %s. Returning 1.",
                    e.getMessage(), table, columns, values));
        }
        return 1; // Unknown
    }

    static List<Object> lookupDimension(String table, Map<String, String> columnsToReturn, String id) throws SQLException, ClassNotFoundException {
        String dimensionLookupStatement = "SELECT (cols) FROM (tbl) WHERE id = ?";
        Class.forName("com.mysql.jdbc.Driver");
        List<Object> data = new ArrayList<>();
//        System.out.println(String.format("[LOG] Looking up from %s, columns %s, id %s", table, Arrays.toString(columnsToReturn.keySet().toArray()), id));

        if (connection == null) {
            connection = DriverManager.getConnection(url, user, pass);
        }

        dimensionLookupStatement = dimensionLookupStatement.replace("(tbl)", table);
        dimensionLookupStatement = dimensionLookupStatement.replace("(cols)", Arrays.toString(columnsToReturn.keySet().toArray()));
        try {
            stmt = connection.prepareStatement(dimensionLookupStatement);
            stmt.setString(1, id);
//            System.out.println("[LOG] Lookup Query: " + dimensionLookupStatement);

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

    static ArrayList<Integer> lookUpDateTime(String timeStr) throws SQLException, ClassNotFoundException {
        ArrayList<Integer> toReturn = new ArrayList<>();

        if (timeStr.contains("T")) {
            toReturn.add(LookupHandler.lookupId("date_d", "full_date",
                    timeStr.substring(0, timeStr.indexOf("T"))));
            String time = timeStr.substring(timeStr.indexOf("T"), timeStr.indexOf("Z")).split(":")[0] + ":00:00";
            toReturn.add(LookupHandler.lookupId("time_d", "time_str", time));
        } else {
            String[] dateParts = timeStr.split(" ");
            toReturn.add(LookupHandler.lookupId("date_d", "full_date", dateParts[0]));
            String time = dateParts[1].split(":")[0] + ":00:00";
            toReturn.add(LookupHandler.lookupId("time_d", "time_str", time));
        }
        return toReturn;
    }

}
