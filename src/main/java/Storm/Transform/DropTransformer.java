package Storm.Transform;

import Storm.AMQPHandler.JSONObjects.Drop;
import Storm.DatabaseHandler.LookupHandler;
import org.apache.storm.tuple.Values;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by charlie on 21/03/17.
 */
public class DropTransformer<Obj> extends Transformer<Obj> {

    @Override
    Values transform(Obj obj) throws SQLException, ClassNotFoundException {
        Drop drop = (Drop) obj;

        Map<String, String> columns = new TreeMap<>();
        columns.put("class_display", "String");
        columns.put("subclass_display", "String");
        List<Object> returned = LookupHandler.customLookUp(
                "select class_display, subclass, subclass_display from drop_class_type_d where class = '" + drop.getDropClass().value + "'", columns);
        if (!returned.isEmpty()) {
            drop.setDropClassDisplay((String) returned.get(0));
            drop.getDropSubClass().value = (String) returned.get(1);
            drop.setDropSubClassDisplay((String) returned.get(2));
        } else {
            drop.setDropClassDisplay("Unknown");
            drop.getDropSubClass().value = "Unknown";
            drop.setDropSubClassDisplay("Unknown");
        }
        drop.getRoute().id = LookupHandler.getScheduleId(drop.getRouteType(), drop.getShopId().value);
        drop.getStatusDisplay().value = LookupHandler.lookupColumn("drop_status_type_d", "class_display", "class", drop.getStatus().value);

        drop.setEventDate(transformDate(drop.getEventDate()));

        Values output = new Values();
        output.add(drop.getReference());
        output.add(drop.getDropClass().value);
        output.add(drop.getDropSubClass().value);
        output.add(drop.getStatus().value);
        output.add(drop.getDropClassDisplay());
        output.add(drop.getDropSubClassDisplay());
        output.add(drop.getStatusDisplay().value);
        output.add(1);
        output.add(drop.getEventDate());
        output.add(drop.getRoute().id);
        output.add(drop.getRouteType());
        output.add(drop.getShopId().value);
        output.add(drop.getRoute().value);

        // return item as list of fields
        return output;
    }
}
