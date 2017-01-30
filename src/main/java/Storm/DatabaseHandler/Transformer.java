package Storm.DatabaseHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by charlie on 30/01/17.
 */
public class Transformer {


    List<Object> transformItem(Item item) {
        System.out.println("[LOG] Transforming Item now..");

        try {
            // should I use storms lookup bolt? or do it manually? perf test the two options
            item.setItemClass(String.valueOf(LookupHandler.lookupId("inv_item_class_type_d", "class", item.getItemClass())));
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        List<Object> output = new ArrayList<>();
        output.add(item.getReference());
        output.add(item.getItemClass());
        output.add(item.getItemSubClass());
        output.add(item.getStatus());
        output.add(item.getItemClass());
        output.add(item.getItemSubClass());
        output.add(item.getStatus());
        output.add(item.getReference());
        output.add(item.getStatedDay());
        output.add(item.getStatedTime());
        output.add(item.getClient());
        output.add(item.getCustomerName());
        output.add(item.getCustAddr());
        output.add(1);
        output.add(item.getEventDate());
        output.add(item.getPostcode());
        output.add(item.getRouteType());

        // return item as list of fields
        return output;
    }
}
