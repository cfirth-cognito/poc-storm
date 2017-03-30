package Storm.AMQPHandler;

import Storm.AMQPHandler.JSONObjects.*;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.commons.lang.StringUtils;
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
        String payload;
        try {
            payload = JsonPath.parse(msg).read("$.payload");
        } catch (PathNotFoundException pfe) {
            log.warn("Payload path not found. Using message body.");
            payload = msg;
        }

        itemState.setReference(parseByPath(payload, "$.transitionMetaDataView.invItemReference"));
        itemState.setMessageRef(parseByPath(payload, "$.transitionMetaDataView.reference"));
        itemState.setStateDateTimeLocal(parseByPath(payload, "$.transitionMetaDataView.eventDate"));
        itemState.getItemStateClass().value = parseByPath(payload, "$.transitionMetaDataView.class");
        itemState.setListRef(parseByPath(payload, "$.transitionMetaDataView.parameters.ManifestNum", "N/A"));
        itemState.getItemStateSubClass().value = parseByPath(payload, "$.transitionMetaDataView.subClass", "N/A");
        itemState.setResourceRef(parseByPath(payload, "$.transitionMetaDataView.resourceReference"));
        itemState.setRouteRef(parseByPath(payload, "$.routeReference"));
        itemState.setRouteType(parseByPath(payload, "$.routeType"));
        itemState.getOutcomeClass().value = (parseByPath(payload, "$.transitionMetaDataView.outcomeCode", "N/A"));
        itemState.getOutcomeSubClass().value = parseByPath(payload, "$.transitionMetaDataView.outcomeText", "N/A");
        itemState.setEtaStartDate(parseByPath(payload, "$.transitionMetaDataView.parameters.ETAStartDT"));
        itemState.setEtaEndDate(parseByPath(payload, "$.transitionMetaDataView.parameters.ETAEndDT"));
        itemState.setAdditionalInfo(parseByPath(payload, "$.transitionMetaDataView.parameters.FreeFormText"));
        itemState.getTrackingPoint().value = parseByPath(payload, "$.lineItems.[0].parameters.TrackingPoint");
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

        /* These aren't actually used in the ListObj ETL? */
//        listObj.getType().value = parseByPath(payload, "$.listMetadata.type");
//        listObj.getSubType().value = parseByPath(payload, "$.listMetadata.subType");
//        listObj.getVanRouteId().value = parseByPath(payload, "$.listMetadata.parameters.VanRouteId");
        /*                                                  */

        listObj.getListClass().value = parseByPath(payload, "$.listMetadata.listClass");
        listObj.setEventDate(parseByPath(payload, "$.listMetadata.eventDate"));
        listObj.getBeginDate().value = parseByPath(payload, "$.beginDate");
        listObj.getSchedule().value = parseByPath(payload, "$.routeReference");
        listObj.getRouteType().value = parseByPath(payload, "$.routeType");
        listObj.getResource().value = parseByPath(payload, "$.listMetadata.parameters.ResourceRef");


        return listObj;
    }

    Drop parseDrop(String msg) {
        Drop drop = new Drop();

        String payload = JsonPath.parse(msg).read("$.payload");

        drop.setReference(parseByPath(payload, "$.itemMetadata.reference"));
        drop.getDropClass().value = parseByPath(payload, "$.itemMetadata.class");
        drop.getStatus().value = parseByPath(payload, "$.status.code");
        drop.setEventDate(parseByPath(payload, "$.itemMetadata.eventDate"));
        drop.setRouteType(parseByPath(payload, "$.routeType"));
        drop.getShopId().value = parseByPath(payload, "$.parcelShopLocation.reference");
        drop.getRoute().value = parseByPath(payload, "$.routeReference");

        return drop;
    }

    DropState parseDropState(String msg) {
        DropState dropState = new DropState();

        String payload;
        try {
            payload = JsonPath.parse(msg).read("$.payload");
        } catch (PathNotFoundException pfe) {
            log.warn("Payload path not found. Using message body.");
            payload = msg;
        }

        dropState.setReference(parseByPath(payload, "$.transitionMetaDataView.invItemReference"));
        dropState.setEventDate(parseByPath(payload, "$.transitionMetaDataView.eventDate"));
        dropState.setMessageRef(parseByPath(payload, "$.transitionMetaDataView.reference"));
        dropState.getList().value = parseByPath(payload, "$.transitionMetaDataView.parameters.ManifestNum", "N/A");
        dropState.getDropStateClass().value = parseByPath(payload, "$.transitionMetaDataView.class");
        dropState.getDropStateSubClass().value = parseByPath(payload, "$.transitionMetaDataView.subClass", "N/A");
        dropState.getResource().value = parseByPath(payload, "$.transitionMetaDataView.resourceReference");
        dropState.getTrackingPoint().value = parseByPath(payload, "$.lineItems.[0].parameters.TrackingPoint");
        dropState.getRouteType().value = parseByPath(payload, "$.routeType");
        dropState.setBillingRef(parseByPath(payload, "$.transitionMetaDataView.parameters.BillingResRef"));
        dropState.getShop().value = (parseByPath(payload, "$.networkNodeLocationView.reference"));
        dropState.getRoute().value = parseByPath(payload, "$.routeReference");
        dropState.setLat(parseByPath(payload, "$.transitionMetaDataView.geographicCoordinates.latitude"));
        dropState.setLongitude(parseByPath(payload, "$.transitionMetaDataView.geographicCoordinates.longitude"));
        dropState.setValidity(parseByPath(payload, "$.transitionMetaDataView.geographicCoordinates.validity"));
        dropState.setDuration(parseByPath(payload, "$.transitionMetaDataView.parameters.Duration"));
        dropState.setCustomerName(parseByPath(payload, "$.contactDetails.customerName"));
        dropState.setDesignRank(parseByPath(payload, "$.transitionMetaDataView.parameters.DesignRank"));
        dropState.getOutcomeClass().value = (parseByPath(payload, "$.transitionMetaDataView.outcomeCode", "N/A"));
        dropState.getOutcomeSubClass().value = parseByPath(payload, "$.transitionMetaDataView.outcomeText", "N/A");

        return dropState;
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
//        if (listObj.getType().value == null)
//            return "ListObj Type failed validation";
        if (listObj.getRouteType().value == null)
            return "Route Type failed validation";
        if (listObj.getEventDate() == null)
            return "Event Date failed validation";
        if (listObj.getSchedule().value == null)
            return "Route Ref failed validation";

        return null;
    }

    String validateDrop(Drop drop) {
        if (StringUtils.isEmpty(drop.getReference()))
            return "Reference failed validation";
        if (StringUtils.isEmpty(drop.getDropClass().value))
            return "Class failed validation";
        if (StringUtils.isEmpty(drop.getStatus().value))
            return "Status failed validation";
        if (StringUtils.isEmpty(drop.getEventDate()))
            return "Event Date failed validation";
        if (StringUtils.isEmpty(drop.getRouteType()))
            return "Route Type failed validation";
//        if (StringUtils.isEmpty(drop.getShopId().value))
//            return "Shop ID failed validation";
        if (StringUtils.isEmpty(drop.getRoute().value))
            return "Route ID failed validation";

        return null;
    }

    String validateDropState(DropState dropState) {
        if (StringUtils.isEmpty(dropState.getReference()))
            return "Reference failed validation";
        if (StringUtils.isEmpty(dropState.getEventDate()))
            return "Event Date failed validation";
        if (StringUtils.isEmpty(dropState.getMessageRef()))
            return "Message Reference failed validation";
        if (StringUtils.isEmpty(dropState.getDropStateClass().value))
            return "State Class failed validation";
        if (StringUtils.isEmpty(dropState.getResource().value))
            return "Status failed validation";
        if (StringUtils.isEmpty(dropState.getRouteType().value))
            return "Route Type failed validation";
        if (StringUtils.isEmpty(dropState.getBillingRef()))
            return "Billing Ref failed validation";
//        if (StringUtils.isEmpty(dropState.getShop().value))
//            return "Shop ID failed validation";
        if (StringUtils.isEmpty(dropState.getRoute().value))
            return "Route ID failed validation";
        if (StringUtils.isEmpty(dropState.getLat()))
            return "Lat failed validation";
        if (StringUtils.isEmpty(dropState.getLongitude()))
            return "Long failed validation";
        if (StringUtils.isEmpty(dropState.getValidity()))
            return "Validity failed validation";

        return null;
    }

    private String parseByPath(String msg, String path) {
        try {
            return String.valueOf(JsonPath.parse(msg).read(path));
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

    private String parseByPath(String msg, String path, String defaultString) {
        try {
            String parsed = JsonPath.parse(msg).read(path);
            if (parsed == null || parsed.isEmpty())
                return defaultString;
            else
                return parsed;
        } catch (PathNotFoundException | NullPointerException e) {
            if (e.getMessage().contains("'null")) { // Null value in JSON - valid, handle properly
                log.debug("Gracefully handling a null json value in message");
                return null;
            } else {
                log.warn(String.format("Path %s not found in message! Returning default.", path));
                return defaultString;
            }
        }
    }

}
