package Storm.DatabaseHandler;


import Storm.Util.PropertiesHolder;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

    private static Cache<String, Object> lookupCache = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .concurrencyLevel(4)
            .maximumSize(1000)
            .build();


    private static Connection checkConnection() throws SQLException {
        try {
            if (connection == null || connection.isClosed() || !connection.isValid(5)) {
                log.info("Creating new connection. Connection: " + connection);
                if (connection != null) {
                    log.info("Creating new connection. Connection was closed: " + connection.isClosed() + " or valid: " + connection.isValid(5));
                    connection.close();
                }
                connection = DriverManager.getConnection(url, user, pass);
            }
        } catch (Exception e) {
            log.error("Caught exception closing existing DB connection. Continuing.");
        }

        if (stmt != null)
            stmt.close();
        return connection;
    }

    // Throw any exceptions we encounter. This ensure's the bolt worker is killed, and another bolt spun up to try again.

    /**
     * Simple lookup. Specify the table, and the column with which the value is looked up vs.
     *
     * @param table  Table to perform the lookup against
     * @param column Column to assert against
     * @param value  Value to look for
     * @return Either the ID of the row found, or 1.
     */
    public static int lookupId(String table, String column, String value) throws ClassNotFoundException, SQLException, ExecutionException {
        String idLookupStatement = "SELECT id FROM (tbl) WHERE (col) = ?";
        Class.forName("com.mysql.jdbc.Driver");
        connection = checkConnection();

        idLookupStatement = idLookupStatement.replace("(tbl)", table).replace("(col)", column);
        try {
            stmt = connection.prepareStatement(idLookupStatement);

            log.debug(String.format("Looking up from %s, column %s, value %s", table, column, value));
            stmt.setString(1, value);

            return (int) lookUpQueryFromCache(stmt.toString()).get(0).getOrDefault("id", 1);
        } catch (SQLException | NullPointerException e) {
            log.debug(String.format("Caught Exception %s looking up id in table %s, column %s, value %s. Returning 1.",
                    e.getMessage(), table, column, value));
        }
        return 1; // Unknown
    }

    /**
     * More complex lookup. Allows to lookup an ID using multiple columns & values.
     *
     * @param table   Table to perform lookup against.
     * @param columns Columns to assert against.
     * @param values  Values to look for.
     * @return ID of found row, or 1.
     */
    public static int lookupId(String table, List<String> columns, List<String> values) throws ClassNotFoundException, SQLException, ExecutionException {
        String idLookupStatement = "SELECT id FROM (tbl) WHERE";
        Class.forName("com.mysql.jdbc.Driver");
        connection = checkConnection();

        idLookupStatement = idLookupStatement.replace("(tbl)", table);
        for (String column : columns) {
            idLookupStatement += " " + column + " = " + "? AND";
        }
        idLookupStatement = idLookupStatement.substring(0, idLookupStatement.length() - 4);

        stmt = connection.prepareStatement(idLookupStatement);
        int count = 1;
        for (String value : values) {
            if (value == null || value.isEmpty())
                value = "N/A";
            stmt.setString(count, value);
            count++;
        }
        ArrayList<Map<String, Object>> resultSet = lookUpQueryFromCache(stmt.toString());
        return (int) resultSet.get(0).getOrDefault("id", 1);

    }

    public static String lookupColumn(String table, String column, String wColumn, String value) throws SQLException, ClassNotFoundException, ExecutionException {
        String idLookupStatement = "SELECT " + column + " FROM (tbl) WHERE (col) = ?";
        Class.forName("com.mysql.jdbc.Driver");
        connection = checkConnection();

        idLookupStatement = idLookupStatement.replace("(tbl)", table).replace("(col)", wColumn);
        stmt = connection.prepareStatement(idLookupStatement);

        log.debug(String.format("Looking up from %s, column %s, wColumn %s, value %s", table, column, wColumn, value));
        stmt.setString(1, value);

        return (String) lookUpQueryFromCache(stmt.toString()).get(0).getOrDefault(column, "Unknown");
    }

    /**
     * Lookup from a dimension, return specified columns
     *
     * @param table           Table to lookup from
     * @param columnsToReturn Columns to return. Format is: [ColumnName, Type]
     * @param id              ID or Reference to look up using
     * @param lookUpColumn    Column to look up using, (id / inv_{}_ref)
     * @return Values from the dimension in the same order as the requested columns
     */
    public static List<Object> lookupDimension(String table, TreeMap<String, String> columnsToReturn, String id, String lookUpColumn) throws SQLException, ClassNotFoundException {
        String dimensionLookupStatement = "SELECT (cols) FROM (tbl) WHERE " + lookUpColumn + " = ?";
        Class.forName("com.mysql.jdbc.Driver");
        List<Object> data = new ArrayList<>();

        connection = checkConnection();


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

            resultSet.close();
            return data;
        } catch (SQLException | NullPointerException e) {
            log.debug(String.format("Caught Exception %s looking up id in table %s, column %s, value %s. Returning 1.",
                    e.getMessage(), table, Arrays.toString(columnsToReturn.keySet().toArray()), id));
        }
        return null; // Unknown
    }

    /**
     * Provide custom SQL, and the columns you then expect to be returned
     *
     * @param sql                  Custom SQL to execute against Database
     * @param columnsBeingReturned Columns you expect to be returned in <Column, Type> format
     * @return
     */
    public static List<Object> customLookUp(String sql, Map<String, String> columnsBeingReturned) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        List<Object> data = new ArrayList<>();

        connection = checkConnection();

        Statement customStatement = connection.createStatement();
        ResultSet results = customStatement.executeQuery(sql);

        while (results.next())
            for (String columnName : columnsBeingReturned.keySet())
                data.add(getValue(columnName, columnsBeingReturned.get(columnName), results));

        results.close();
        return data;
    }

    @SuppressWarnings("unchecked cast")
    private static ArrayList<Map<String, Object>> lookUpQueryFromCache(String lookupStatement) throws ExecutionException {
        return (ArrayList<Map<String, Object>>) lookupCache.get(lookupStatement, () -> {
            ResultSet resultSet = stmt.executeQuery();
            ResultSetMetaData md = resultSet.getMetaData();
            ArrayList<HashMap<String, Object>> rows = new ArrayList<>();
            int columnCount = md.getColumnCount();

            while (resultSet.next()) {
                HashMap<String, Object> row = new HashMap<>(columnCount);

                for (int c = 1; c <= columnCount; ++c)
                    row.put(md.getColumnName(c), resultSet.getObject(c));

                rows.add(row);
            }


            return rows;

        });
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

    public static ArrayList<Integer> lookUpDateTime(String timeStr) throws SQLException, ClassNotFoundException, ExecutionException {
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

    public static int getScheduleId(String routeType, String routeRef) throws SQLException, ClassNotFoundException, ExecutionException {
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
