package Storm.AMQPHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import Storm.AMQPHandler.JSONObj.Item.ItemState;
import Storm.Util.Field;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

/**
 * Created by charlie on 30/01/17.
 */
public class Parser {

    Item parseItem(String msg) {

        Item item = new Item();
        String payload = JsonPath.parse(msg).read("$.payload");


        item.setReference(parseByPath(payload, "$.itemMetadata.reference"));
        item.setItemClass(parseByPath(payload, "$.itemMetadata.class"));
        item.setItemSubClass(parseByPath(payload, "$.itemMetadata.subClass"));
        item.setCustomerName(parseByPath(payload, "$.contactDetails.name"));
        item.setStatus("test");
        item.setClient(parseByPath(payload, "$.itemMetadata.parameters.ClientId"));
        item.setStatedDay(parseByPath(payload, "$.itemMetadata.parameters.StatedDay"));
        item.setStatedTime(parseByPath(payload, "$.itemMetadata.parameters.StatedTime"));
        item.setEventDate(parseByPath(payload, "$.itemMetadata.eventDate"));
        item.setRouteRef(parseByPath(payload, "$.routeReference"));
        item.setRouteType(parseByPath(payload, "$.routeType"));
        item.setLineItemId(parseByPath(payload, "$.itemMetadata.lineItemViews[0].id"));
        item.setCustAddr(parseByPath(payload, "$.customerLocation.address.line1"));
        item.setPostcode(parseByPath(payload, "$.customerLocation.address.postCode"));
        item.setShopReference(parseByPath(payload, "$.parcelShopLocation.reference"));

        return item;
    }

    ItemState parseItemState(String msg) {

        ItemState itemState = new ItemState();
        String payload = JsonPath.parse(msg).read("$.payload");

        itemState.setReference(parseByPath(payload, "$.transitionMetaDataView.invItemReference"));
        itemState.setStateDateTimeLocal(parseByPath(payload, "$.transitionMetadata.eventDate"));
        itemState.getItemStateClass().value = parseByPath(payload, "$.transitionMetaDataView.class");
        itemState.getManifested().value = parseByPath(payload, "$.transitionMetaDataView.parameters.ManifestNum");
        itemState.getItemStateSubClass().value = parseByPath(payload, "$.transitionMetadata.subClass");
        itemState.setResourceRef(parseByPath(payload, "$.transitionMetaDataView.resourceReference"));
        itemState.setRouteRef(parseByPath(payload, "$.routeReference"));
        itemState.setRouteType(parseByPath(payload, "$.routeType"));
        itemState.getOutcomeClass().value = (parseByPath(payload, "$.transitionMetaDataView.outcomeCode"));
        itemState.getOutcomeSubClass().value = parseByPath(payload, "$.transitionMetaDataView.outcomeText");
        itemState.setEtaStartDate(parseByPath(payload, "$.transitionMetaDataView.parameters.ETAStartDT"));
        itemState.setEtaEndDate(parseByPath(payload, "$.transitionMetaDataView.parameters.ETAEndDT"));
        itemState.setAdditionalInfo(parseByPath(payload, "$.transitionMetaDataView.parameters.FreeFormText"));
        itemState.getTrackingPoint().value = parseByPath(payload, "$.transitionMetaDataView.parameters.TrackingPoint");
        itemState.setFromShop(parseByPath(payload, "$.transitionMetaDataView.parameters.FromShopId"));
        itemState.setToShop((parseByPath(payload, "$.transitionMetaDataView.parameters.ToShopId")));
        itemState.setBillingRef(parseByPath(payload, "$.transitionMetaDataView.parameters.BillingResRef"));
        itemState.getShopReference().value = (parseByPath(payload, "$.networkNodeLocationView.reference"));

        return itemState;
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


    String validateItemState(ItemState itemState) {
        if (itemState.getReference() == null)
            return "Reference failed validation";
        if (itemState.getItemClass() == null)
            return "Class failed validation";
        if (itemState.getStateDateTimeLocal() == null)
            return "EventDate failed validation";
        if (itemState.getRouteRef() == null)
            return "RouteRef failed validation";
        if (itemState.getRouteType() == null)
            return "RouteType failed validation";
        if (itemState.getResourceRef() == null)
            return "Resource Reference failed validation";
        return null;
    }

    private String parseByPath(String msg, String path) {
        try {
            return JsonPath.parse(msg).read(path);
        } catch (PathNotFoundException e) {
            if (e.getMessage().contains("'null")) { // Null value in JSON - valid, handle properly
//                System.out.println("[LOG] Handling 'null' json value.");
                return null;
            } else {
//                System.out.println(String.format("[LOG] Path not found %s", path));
            }
        }
        return null;
    }

}
