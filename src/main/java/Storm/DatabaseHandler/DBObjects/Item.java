package Storm.DatabaseHandler.DBObjects;

import com.google.common.collect.Lists;
import org.apache.storm.jdbc.common.Column;

import java.sql.Types;
import java.util.List;

/**
 * Created by charlie on 30/01/17.
 */
public class Item {
    private static List<Column> columns = Lists.newArrayList(
            new Column("inv_item_ref", Types.VARCHAR),
            new Column("inv_item_class", Types.VARCHAR),
            new Column("inv_item_subclass", Types.VARCHAR),
            new Column("inv_item_status", Types.VARCHAR),
            new Column("inv_item_class_display", Types.VARCHAR),
            new Column("inv_item_subclass_display", Types.VARCHAR),
            new Column("inv_item_status_display", Types.VARCHAR),
            new Column("barcode", Types.VARCHAR),
            new Column("stated_day", Types.VARCHAR),
            new Column("stated_time", Types.VARCHAR),
            new Column("client", Types.VARCHAR),
            new Column("customer_name", Types.VARCHAR),
            new Column("customer_address_1", Types.VARCHAR),
            new Column("version", Types.INTEGER),
            new Column("event_date", Types.VARCHAR),
            new Column("postcode", Types.VARCHAR),
            new Column("route_type", Types.VARCHAR));

    public static List<Column> getColumns() {
        return columns;
    }

    public static String columnsToString() {
        return "inv_item_ref, inv_item_class, inv_item_subclass, inv_item_status, " +
                "inv_item_class_display, inv_item_subclass_display, inv_item_status_display, " +
                " barcode, stated_day, stated_time, client, customer_name, customer_address_1," +
                "version, event_date, postcode, route_type";
    }
}
