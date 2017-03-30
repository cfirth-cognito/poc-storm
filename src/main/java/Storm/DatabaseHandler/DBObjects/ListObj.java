package Storm.DatabaseHandler.DBObjects;

import com.google.common.collect.Lists;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.ArrayList;

/**
 * Created by charlie on 17/02/17.
 */
public class ListObj {
    private static java.util.List<Column> columns = Lists.newArrayList(
            new Column("inv_list_ref", Types.VARCHAR),
            new Column("inv_list_class", Types.VARCHAR),
            new Column("inv_list_class_display", Types.VARCHAR),
            new Column("sort_order", Types.INTEGER),
            new Column("version", Types.INTEGER),
            new Column("event_date", Types.VARCHAR),
            new Column("begin_date", Types.VARCHAR),
            new Column("schedule_mgmt_id", Types.INTEGER),
            new Column("begin_date_id", Types.INTEGER),
            new Column("route_type", Types.VARCHAR));

    public static java.util.List<Column> getColumns() {
        return columns;
    }

    public static String columnsToString() {
        String columnString = "";
        for (Column col : columns) {
            if (columns.indexOf(col) != columns.size() - 1)
                columnString += col.getColumnName() + ",";
            else
                columnString += col.getColumnName();
        }
        return columnString;
    }

    public static Fields fields() {
//        ListObj<String> fields = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
        java.util.List<String> fields = new ArrayList<>();
        for (Column column : columns) {
            fields.add(column.getColumnName());
        }
        return new Fields(fields);
    }

    public static String getPlaceholders() {
        String placeholders = "";
        for (int i = 0; i < columns.size(); i++) {
            placeholders += "?,";
        }
        return placeholders.substring(0, placeholders.lastIndexOf(","));
    }
}

