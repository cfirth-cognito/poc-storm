package Storm.DatabaseHandler.DBObjects;

import com.google.common.collect.Lists;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by charlie on 30/01/17.
 */
public class Drop {
    private static List<Column> columns = Lists.newArrayList(
            new Column("drop_ref", Types.VARCHAR),
            new Column("drop_class", Types.VARCHAR),
            new Column("drop_subclass", Types.VARCHAR),
            new Column("drop_status", Types.VARCHAR),
            new Column("drop_class_display", Types.VARCHAR),
            new Column("drop_subclass_display", Types.VARCHAR),
            new Column("drop_status_display", Types.VARCHAR),
            new Column("version", Types.INTEGER),
            new Column("event_date", Types.VARCHAR),
            new Column("schedule_mgmt_id", Types.INTEGER),
            new Column("route_type", Types.VARCHAR),
            new Column("shop_id", Types.VARCHAR),
            new Column("route_id", Types.INTEGER));

    public static List<Column> getColumns() {
        return columns;
    }

    public static String columnsToString() {
        String columnString = "";
        for (Column col : columns) {
            if (columns.indexOf(col) != columns.size() - 1)
                columnString += "`" + col.getColumnName() + "`,";
            else
                columnString += "`" + col.getColumnName() + "`";
        }
        return columnString;
    }

    public static Fields fields() {
//        ListObj<String> fields = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
        List<String> fields = new ArrayList<>();
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
