package Storm.AMQPHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

/**
 * Created by charlie on 30/01/17.
 */
public class Parser {

    public Item parseItem(String msg) {

        Item item = new Item();
        String payload = JsonPath.parse(msg).read("$.payload");

        item.setReference(parseByPath(payload, ".itemMetadata.reference"));
        item.setItemClass(parseByPath(payload, ".itemMetadata.class"));
        item.setItemSubClass(parseByPath(payload, ".itemMetadata.subClass"));
        item.setCustomerName(parseByPath(payload, ".contactDetails.name"));
        item.setStatus("test");
        item.setClient(parseByPath(payload, ".itemMetadata.parameters.ClientId"));
        item.setStatedDay(parseByPath(payload, ".itemMetadata.parameters.StatedDay"));
        item.setStatedTime(parseByPath(payload, ".itemMetadata.parameters.StatedTime"));
        item.setEventDate(parseByPath(payload, ".itemMetadata.eventDate"));
        item.setRouteRef(parseByPath(payload, ".routeReference"));
        item.setRouteType(parseByPath(payload, ".routeType"));
        item.setLineItemId(parseByPath(payload, ".itemMetadata.lineItemViews[0].id"));
        item.setCustAddr(parseByPath(payload, ".customerLocation.address.line1"));
        item.setPostcode(parseByPath(payload, ".customerLocation.address.postCode"));
        item.setShopReference(parseByPath(payload, ".parcelShopLocation.reference"));

        return item;
    }

    // Can do this in the Setters of the various Objects themselves.
    // Doing it here for now, for visibility
    String validateItem(Item item) {
        if (item.getReference() == null)
            return "Reference failed validation";
        if (item.getItemClass() == null)
            return "Class failed validation";
        if (item.getItemSubClass() == null)
            return "SubClass failed validation";
        if (item.getCustomerName() == null)
            return "CustomerName failed validation";
        if (item.getStatus() == null)
            return "Status failed validation";
        if (item.getClient() == null)
            return "Client failed validation";
        if (item.getEventDate() == null)
            return "EventDate failed validation";
        if (item.getRouteRef() == null)
            return "RouteRef failed validation";
        if (item.getRouteType() == null)
            return "RouteType failed validation";

        return null;
    }

    private String parseByPath(String msg, String path) {
        String payload = "$";
        try {
            return JsonPath.parse(msg).read(payload + path);
        } catch (PathNotFoundException e) {
            if (e.getMessage().contains("'null")) { // Null value in JSON - valid, handle properly
                System.out.println("[LOG] Handling 'null' json value.");
                return null;
            } else {
                e.printStackTrace();
            }
        }
        return null;
    }

}
