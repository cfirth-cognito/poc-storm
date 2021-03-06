package Storm.Transform;

import Storm.AMQPHandler.JSONObjects.Drop;
import Storm.DatabaseHandler.LookupHandler;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by charlie on 21/03/17.
 */
public class DropTransformer extends Transformer<Drop> {
    private static final Logger log = LoggerFactory.getLogger(DropTransformer.class);

    @Override
    public Values transform(Drop drop) throws SQLException, ClassNotFoundException {
        log.warn("Transforming drop.");

        Map<String, String> columns = new TreeMap<>();
        columns.put("class_display", "String");
        columns.put("subclass", "String");
        columns.put("subclass_display", "String");

        List<Object> returned = LookupHandler.customLookUp(
                "SELECT class_display, subclass, subclass_display FROM drop_class_type_d WHERE class = '" + drop.getDropClass().value + "'", columns);
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
        output.add(Integer.valueOf(drop.getRoute().value));

        // return item as list of fields
        log.warn("Emitting drop.");
        return output;
    }
}
