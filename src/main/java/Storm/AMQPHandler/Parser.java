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

    private String parseByPath(String msg, String path) {
        String payload = "$";
        try {
            return JsonPath.parse(msg).read(payload + path);
        } catch (PathNotFoundException e) {
            System.out.println("[LOG] CAUGHT EXCEPTION");
            System.out.println("[LOG] " + e.getMessage());
            if (e.getMessage().contains("'null")) { // Null value in JSON - valid, handle properly
                return null;
            } else {
                e.printStackTrace();
            }
        }
        return null;
    }

}
