package Storm.DatabaseHandler.DBObjects;

import com.google.common.collect.Lists;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by charlie on 30/01/17.
 */
public class ItemState {
    private static List<Column> columns = Lists.newArrayList(
            new Column("inv_item_ref", Types.VARCHAR),
            new Column("state_datetime_local", Types.VARCHAR),
            new Column("state_date_id", Types.INTEGER),
            new Column("state_time_id", Types.INTEGER),
            new Column("message_ref", Types.VARCHAR),
            new Column("inv_item_id", Types.INTEGER),
            new Column("inv_list_id", Types.INTEGER),
            new Column("inv_list_ref", Types.VARCHAR),
            new Column("inv_item_class_type_id", Types.INTEGER),
            new Column("inv_item_state_type_id", Types.INTEGER),
            new Column("resource_mgmt_id", Types.INTEGER),
            new Column("schedule_mgmt_id", Types.INTEGER),
            new Column("network_id", Types.INTEGER),
            new Column("geography_id", Types.INTEGER),
            new Column("state_transition_counter", Types.INTEGER),
            new Column("begin_date_id", Types.INTEGER),
            new Column("eta_start_datetime_local", Types.VARCHAR),
            new Column("eta_end_datetime_local", Types.VARCHAR),
            new Column("additional_info", Types.VARCHAR),
            new Column("inv_item_status_type_id", Types.INTEGER),
            new Column("manifested_id", Types.INTEGER),
            new Column("tracking_point_id", Types.INTEGER),
            new Column("route_type_id", Types.INTEGER),
            new Column("from_shop_id", Types.INTEGER),
            new Column("to_shop_id", Types.INTEGER),
            new Column("billing_ref", Types.VARCHAR));

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
        List<String> fields = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
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
