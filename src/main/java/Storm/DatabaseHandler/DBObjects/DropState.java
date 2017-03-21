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

public class DropState {
    private static List<Column> columns = Lists.newArrayList(
            new Column("drop_ref", Types.VARCHAR),
            new Column("state_datetime_local", Types.VARCHAR),
            new Column("state_date_id", Types.INTEGER),
            new Column("state_time_id", Types.INTEGER),
            new Column("message_ref", Types.VARCHAR),
            new Column("drop_id", Types.INTEGER),
            new Column("inv_list_id", Types.INTEGER),
            new Column("inv_list_ref", Types.VARCHAR),
            new Column("drop_class_type_id", Types.INTEGER),
            new Column("drop_state_type_id", Types.INTEGER),
            new Column("resource_mgmt_id", Types.INTEGER),
            new Column("schedule_mgmt_id", Types.INTEGER),
            new Column("network_id", Types.INTEGER),
            new Column("geography_id", Types.INTEGER),
            new Column("state_transition_counter", Types.INTEGER),
            new Column("begin_date_id", Types.INTEGER),
            new Column("drop_status_type_id", Types.INTEGER),
            new Column("manifested_id", Types.INTEGER),
            new Column("tracking_point_id", Types.INTEGER),
            new Column("route_type_id", Types.INTEGER),
            new Column("billing_ref", Types.VARCHAR),
            new Column("shop_id", Types.INTEGER),
            new Column("route_id", Types.INTEGER),
            new Column("lat", Types.DOUBLE),
            new Column("long", Types.DOUBLE),
            new Column("validity", Types.VARCHAR),
            new Column("duration", Types.VARCHAR),
            new Column("customer_name", Types.VARCHAR),
            new Column("design_rank", Types.INTEGER));

    public static List<Column> getColumns() {
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
