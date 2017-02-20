package Storm.AMQPHandler;

import Storm.AMQPHandler.JSONObjects.Item;
import Storm.AMQPHandler.JSONObjects.ItemState;
import Storm.AMQPHandler.JSONObjects.ListObj;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by charlie on 30/01/17.
 */
public class Parser {
    private static final Logger log = LoggerFactory.getLogger(Parser.class);

    Item parseItem(String msg) {
        Item item = new Item();
        String payload = JsonPath.parse(msg).read("$.payload");

        item.setReference(parseByPath(payload, "$.itemMetadata.reference"));
        item.setItemClass(parseByPath(payload, "$.itemMetadata.class"));
        item.setItemSubClass(parseByPath(payload, "$.itemMetadata.subClass"));
        item.setCustomerName(parseByPath(payload, "$.contactDetails.name"));
        item.getStatus().value = parseByPath(payload, "$.status.code");
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
        itemState.setMessageRef(parseByPath(payload, "$.transitionMetaDataView.reference"));
        itemState.setStateDateTimeLocal(parseByPath(payload, "$.transitionMetaDataView.eventDate"));
        itemState.getItemStateClass().value = parseByPath(payload, "$.transitionMetaDataView.class");
        itemState.setListRef(parseByPath(payload, "$.transitionMetaDataView.parameters.ManifestNum"));
        itemState.getItemStateSubClass().value = parseByPath(payload, "$.transitionMetaDataView.subClass");
        itemState.setResourceRef(parseByPath(payload, "$.transitionMetaDataView.resourceReference"));
        itemState.setRouteRef(parseByPath(payload, "$.routeReference"));
        itemState.setRouteType(parseByPath(payload, "$.routeType"));
        itemState.getOutcomeClass().value = (parseByPath(payload, "$.transitionMetaDataView.outcomeCode"));
        itemState.getOutcomeSubClass().value = parseByPath(payload, "$.transitionMetaDataView.outcomeText");
        itemState.setEtaStartDate(parseByPath(payload, "$.transitionMetaDataView.parameters.ETAStartDT"));
        itemState.setEtaEndDate(parseByPath(payload, "$.transitionMetaDataView.parameters.ETAEndDT"));
        itemState.setAdditionalInfo(parseByPath(payload, "$.transitionMetaDataView.parameters.FreeFormText"));
        itemState.getTrackingPoint().value = parseByPath(payload, "$.transitionMetaDataView.parameters.TrackingPoint");
        itemState.getFromShop().value = (parseByPath(payload, "$.transitionMetaDataView.parameters.FromShopId"));
        itemState.getToShop().value = ((parseByPath(payload, "$.transitionMetaDataView.parameters.ToShopId")));
        itemState.setBillingRef(parseByPath(payload, "$.transitionMetaDataView.parameters.BillingResRef"));
        itemState.getShopReference().value = (parseByPath(payload, "$.networkNodeLocationView.reference"));
        itemState = replaceItemStateValues(itemState);

        return itemState;
    }

    ListObj parseList(String msg) {
        ListObj listObj = new ListObj();
        String payload = JsonPath.parse(msg).read("$.payload");

        listObj.setReference(parseByPath(payload, "$.listMetadata.reference"));
        listObj.getType().value = parseByPath(payload, "$.listMetadata.type");
        listObj.getSubType().value = parseByPath(payload, "$.listMetadata.subType");
        listObj.getListClass().value = parseByPath(payload, "$.listMetadata.listClass");
        listObj.setEventDate(parseByPath(payload, "$.listMetadata.eventDate"));
        listObj.getBeginDate().value = parseByPath(payload, "$.beginDate");
        listObj.getSchedueleRef().value = parseByPath(payload, "$.routeReference");
        listObj.getRouteType().value = parseByPath(payload, "$.routeType");
        listObj.getResource().value = parseByPath(payload, "$.listMetadata.parameters.ResourceRef");
        listObj.getVanRouteId().value = parseByPath(payload, "$.listMetadata.parameters.VanRouteId");

        return listObj;
    }


    private ItemState replaceItemStateValues(ItemState itemState) {
        if (itemState.getEtaEndDate() != null && itemState.getEtaEndDate().isEmpty())
            itemState.setEtaEndDate(null);
        if (itemState.getEtaStartDate() != null && itemState.getEtaStartDate().isEmpty())
            itemState.setEtaStartDate(null);
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
        if (itemState.getItemClass().value == null)
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

    String validateList(ListObj listObj) {
        if (listObj.getReference() == null)
            return "Reference failed validation";
        if (listObj.getBeginDate().value == null)
            return "BeginDate failed validation";
        if (listObj.getListClass().value == null)
            return "ListObj Class failed validation";
        if (listObj.getType().value == null)
            return "ListObj Type failed validation";
        if (listObj.getRouteType().value == null)
            return "Route Type failed validation";
        if (listObj.getEventDate() == null)
            return "Event Date failed validation";
        if (listObj.getSchedueleRef().value == null)
            return "Route Ref failed validation";

        return null;
    }

    private String parseByPath(String msg, String path) {
        try {
            return JsonPath.parse(msg).read(path);
        } catch (PathNotFoundException e) {
            if (e.getMessage().contains("'null")) { // Null value in JSON - valid, handle properly
                log.debug("Gracefully handling a null json value in message");
                return null;
            } else {
                log.warn(String.format("Path %s not found in message!", path));
                // todo: null -> n/a
                return null;
            }
        }
    }

}
