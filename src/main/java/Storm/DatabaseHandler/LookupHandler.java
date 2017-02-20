package Storm.DatabaseHandler;


import Storm.Util.PropertiesHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Created by Charlie on 28/01/2017.
 */
public class LookupHandler {
    private static final Logger log = LoggerFactory.getLogger(LookupHandler.class);

    private static String url = String.format("jdbc:mysql://%s:%s/%s",
            PropertiesHolder.databaseHost, PropertiesHolder.databasePort, PropertiesHolder.databaseSchema);
    private static String user = PropertiesHolder.databaseUser;
    private static String pass = PropertiesHolder.databasePass;

    private static Connection connection;
    private static PreparedStatement stmt;

    // Throw any exceptions we encounter. This ensure's the bolt worker is killed, and another bolt spun up to try again.
    public static int lookupId(String table, String column, String value) throws ClassNotFoundException, SQLException {
        String idLookupStatement = "SELECT id FROM (tbl) WHERE (col) = ?";
        Class.forName("com.mysql.jdbc.Driver");
        if (connection == null || connection.isClosed() || !connection.isValid(5))
            connection = DriverManager.getConnection(url, user, pass);

        idLookupStatement = idLookupStatement.replace("(tbl)", table).replace("(col)", column);
        try {
            stmt = connection.prepareStatement(idLookupStatement);

            log.debug(String.format("Looking up from %s, column %s, value %s", table, column, value));
            stmt.setString(1, value);

            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException | NullPointerException e) {
            log.debug(String.format("Caught Exception %s looking up id in table %s, column %s, value %s. Returning 1.",
                    e.getMessage(), table, column, value));
        }
        return 1; // Unknown
    }

    public static int lookupId(String table, List<String> columns, List<String> values) throws ClassNotFoundException, SQLException {
        String idLookupStatement = "SELECT id FROM (tbl) WHERE";
        Class.forName("com.mysql.jdbc.Driver");
        if (connection == null || connection.isClosed() || !connection.isValid(5))
            connection = DriverManager.getConnection(url, user, pass);

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
            log.debug(String.format("Caught Exception %s looking up id in table %s, columns %s, values %s. Returning 1.",
                    e.getMessage(), table, columns, values));
        }
        return 1; // Unknown
    }

    public static String lookupColumn(String table, String column, String wColumn, String value) throws SQLException, ClassNotFoundException {
        String idLookupStatement = "SELECT " + column + " FROM (tbl) WHERE (col) = ?";
        Class.forName("com.mysql.jdbc.Driver");
        if (connection == null || connection.isClosed() || !connection.isValid(5))
            connection = DriverManager.getConnection(url, user, pass);

        idLookupStatement = idLookupStatement.replace("(tbl)", table).replace("(col)", wColumn);
        try {
            stmt = connection.prepareStatement(idLookupStatement);

            log.debug(String.format("Looking up from %s, column %s, wColumn %s, value %s", table, column, wColumn, value));
            stmt.setString(1, value);

            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                return resultSet.getString(1);
            }
        } catch (SQLException | NullPointerException e) {
            log.debug(String.format("Caught Exception %s looking up id in table %s, column %s, value %s. Returning 1.",
                    e.getMessage(), table, column, value));
        }
        return "Unknown"; // Unknown
    }

    /**
     * Lookup from a dimension, return specified columns
     *
     * @param table           Table to lookup from
     * @param columnsToReturn Columns to return. Format is: [ColumnName, Type]
     * @param id              ID or Reference to look up using
     * @param lookUpColumn    Column to look up using, (id / inv_{}_ref)
     * @return Values from the dimension in the same order as the requested columns
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static List<Object> lookupDimension(String table, TreeMap<String, String> columnsToReturn, String id, String lookUpColumn) throws SQLException, ClassNotFoundException {
        String dimensionLookupStatement = "SELECT (cols) FROM (tbl) WHERE " + lookUpColumn + " = ?";
        Class.forName("com.mysql.jdbc.Driver");
        List<Object> data = new ArrayList<>();

        if (connection == null || connection.isClosed() || !connection.isValid(2))
            connection = DriverManager.getConnection(url, user, pass);


        dimensionLookupStatement = dimensionLookupStatement.replace("(tbl)", table);
        dimensionLookupStatement = dimensionLookupStatement.replace("(cols)",
                Arrays.toString(columnsToReturn.keySet().toArray())
                        .replace("[", "")
                        .replace("]", ""));
        try {
            stmt = connection.prepareStatement(dimensionLookupStatement);
            stmt.setString(1, id);

            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next())
                for (Map.Entry column : columnsToReturn.entrySet())
                    data.add(getValue(column.getKey().toString(), column.getValue().toString(), resultSet));


            return data;
        } catch (SQLException | NullPointerException e) {
            log.debug(String.format("Caught Exception %s looking up id in table %s, column %s, value %s. Returning 1.",
                    e.getMessage(), table, Arrays.toString(columnsToReturn.keySet().toArray()), id));
        }
        return null; // Unknown
    }

    public static List<Object> customLookUp(String sql, Map<String, String> columnsBeingReturned) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        List<Object> data = new ArrayList<>();

        if (connection == null || connection.isClosed() || !connection.isValid(5))
            connection = DriverManager.getConnection(url, user, pass);

        Statement customStatement = connection.createStatement();
        ResultSet results = customStatement.executeQuery(sql);

        while (results.next())
            for (String columnName : columnsBeingReturned.keySet())
                data.add(getValue(columnName, columnsBeingReturned.get(columnName), results));

        return data;
    }

    private static Object getValue(String columnName, String type, ResultSet resultSet) throws SQLException {
        switch (type) {
            case "String":
                return resultSet.getString(columnName);
            case "Integer":
                return resultSet.getInt(columnName);
            case "Date":
                // todo: convert date to string
                return resultSet.getDate(columnName);
            default:
                throw new IllegalArgumentException("Unhandled type passed into GetValue.");
        }
    }

    public static ArrayList<Integer> lookUpDateTime(String timeStr) throws SQLException, ClassNotFoundException {
        ArrayList<Integer> toReturn = new ArrayList<>();

        // todo: just .length? instead of looking for Z/+/-
        if (timeStr.contains("T")) {
            toReturn.add(LookupHandler.lookupId("date_d", "full_date",
                    timeStr.substring(0, timeStr.indexOf("T"))));
            String time = timeStr.substring(timeStr.indexOf("T") + 1).split(":")[0] + ":00:00";
            toReturn.add(LookupHandler.lookupId("time_d", "time_str", time));
        } else {
            String[] dateParts = timeStr.split(" ");
            toReturn.add(LookupHandler.lookupId("date_d", "full_date", dateParts[0]));
            String time = dateParts[1].split(":")[0] + ":00:00";
            toReturn.add(LookupHandler.lookupId("time_d", "time_str", time));
        }
        return toReturn;
    }

    public static int getScheduleId(String routeType, String routeRef) throws SQLException, ClassNotFoundException {
        switch (routeType) {
            case "VANROUTE":
                return LookupHandler.lookupId("schedule_management_dh", "courier_round", routeRef);
            case "ROUND":
                if (routeRef == null || routeRef.equalsIgnoreCase("null"))
                    return 1;
                else
                    return LookupHandler.lookupId("schedule_management_dh", "parcelshop_tier5", routeRef);
            default:
                return 1;
        }
    }

}
