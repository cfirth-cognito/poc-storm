package Storm.DatabaseHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by charlie on 30/01/17.
 */
public class Transformer {

    /* should I use storms lookup bolt?
        or do it manually?
        perf test the two options
     */
    List<Object> transformItem(Item item) {
        System.out.println("[LOG] Transforming Item now..");

        try {

            item.setItemClassDisplay(String.valueOf(LookupHandler.lookupId("inv_item_class_type_d", "class_display", item.getItemClass())));
            item.setItemSubClassDisplay(String.valueOf(LookupHandler.lookupId("inv_item_class_type_d", "subclass_display", item.getItemSubClass())));

            item.setClientId(LookupHandler.lookupId("clients_d", "client_code", item.getClient()));

            switch (item.getRouteType()) {
                case "VANROUTE":
                    item.setScheduleId(LookupHandler.lookupId("schedule_management_dh", "courier_round", item.getRouteRef()));
                    break;
                case "ROUND":
                    if (item.getRouteRef().equalsIgnoreCase("null")) {
                        item.setScheduleId(1);
                    } else {
                        item.setScheduleId(LookupHandler.lookupId("schedule_management_dh", "parcelshop_tier5", item.getRouteRef()));
                    }
                    break;

            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        List<Object> output = new ArrayList<>();
        output.add(item.getReference());
        output.add(item.getItemClass());
        output.add(item.getItemSubClass());
        output.add(item.getStatus());
        output.add(item.getItemClassDisplay());
        output.add(item.getItemSubClassDisplay());
        output.add(item.getStatus());
        output.add(item.getReference());
        output.add(item.getStatedDay());
        output.add(item.getStatedTime());
        output.add(item.getClientId());
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
