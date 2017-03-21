package Storm.Transform;

import Storm.AMQPHandler.JSONObjects.Item;
import Storm.DatabaseHandler.LookupHandler;
import org.apache.storm.tuple.Values;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by charlie on 21/03/17.
 */
public class ItemTransformer<Obj> extends Transformer<Obj> {

    @Override
    public Values transform(Obj obj) {
        Item item = (Item) obj;
        try {
            Map<String, String> columns = new TreeMap<>();
            columns.put("class_display", "String");
            columns.put("subclass_display", "String");
            List<Object> returned = LookupHandler.customLookUp("select class_display, subclass_display from inv_item_class_type_d where class = '" + item.getItemClass() + "'" +
                    " AND subclass = '" + item.getItemSubClass() + "'", columns);
            if (!returned.isEmpty()) {
                item.setItemClassDisplay((String) returned.get(0));
                item.setItemSubClassDisplay((String) returned.get(1));
            } else {
                item.setItemClassDisplay("Unknown");
                item.setItemSubClassDisplay("Unknown");
            }
            item.setClientId(LookupHandler.lookupId("clients_d", "client_code", item.getClient()));
            item.setScheduleId(LookupHandler.getScheduleId(item.getRouteType(), item.getRouteRef()));

            String status = item.getStatus().value;
            item.getStatusDisplay().value = LookupHandler.lookupColumn("inv_item_status_type_d", "class_display", "class", status);
            if (item.getStatusDisplay().value.equalsIgnoreCase("unknown") && status.length() > 0) {
                String[] tempStatus = status.split("_");
                if (tempStatus.length > 1) {
                    for (int i = 0; i < tempStatus.length; i++) {
                        tempStatus[i] = tempStatus[i].substring(0, 1) + tempStatus[i].substring(1).toLowerCase();
                    }
                    status = tempStatus[0] + " " + tempStatus[1];
                } else {
                    status = (status.substring(0, 1) + status.substring(1).toLowerCase());
                }

                item.getStatusDisplay().value = status;
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        item.setStatedDay(transformStatedDay(item.getStatedDay()));
        item.setStatedTime(transformStatedTime(item.getStatedTime()));
        item.setEventDate(transformDate(item.getEventDate()));

        Values output = new Values();
        output.add(item.getReference());
        output.add(item.getItemClass());
        output.add(item.getItemSubClass());
        output.add(item.getStatus().value);
        output.add(item.getItemClassDisplay());
        output.add(item.getItemSubClassDisplay());
        output.add(item.getStatusDisplay().value);
        output.add(item.getReference());
        output.add(item.getStatedDay());
        output.add(item.getStatedTime());
        output.add(item.getClient());
        output.add(item.getCustomerName());
        output.add(item.getCustAddr());
        output.add(1);
        output.add(item.getEventDate());
        output.add(item.getScheduleId());
        output.add(item.getPostcode());
        output.add(item.getClientId());
        output.add(item.getRouteType());

        // return item as list of fields
        return output;
    }




}
