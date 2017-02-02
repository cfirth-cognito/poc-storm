package Storm.DatabaseHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import Storm.AMQPHandler.JSONObj.Item.ItemState;
import org.apache.storm.tuple.Values;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by charlie on 30/01/17.
 */
public class Transformer {

    Values transformItem(Item item) {
//        System.out.println("[LOG] Transforming Item now..");

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

        item.setEventDate(item.getEventDate().replace("Z", "").replace("T", " "));

        Values output = new Values();
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

    Values transformItemState(ItemState itemState) {
        System.out.println("[LOG] Transforming Item State now..");

        try {
            if (itemState.getItemId() == null)
                itemState.setItemId(LookupHandler.lookupId("inv_item_d", "inv_item_ref", itemState.getReference()));
            itemState.getItemClass().id = LookupHandler.lookupId("inv_item_class_type_d", Arrays.asList("class", "subclass")
                    , Arrays.asList(itemState.getItemClass().value, itemState.getItemSubClass().value));
            itemState.getItemStateClass().id = LookupHandler.lookupId("inv_item_state_type_d", Arrays.asList("class", "subclass"),
                    Arrays.asList(itemState.getItemStateClass().value, itemState.getItemStateSubClass().value));
            itemState.setStatusId(LookupHandler.lookupId("inv_item_status_type_d", "class", itemState.getStatus()));

//            2017-02-02 11:07:47.120

            ArrayList<Integer> dateTimeIds = LookupHandler.lookUpDateTime(itemState.getStateDateTimeLocal());
            itemState.setStateDateId(dateTimeIds.get(0));
            itemState.setStateTimeId(dateTimeIds.get(1));

            itemState.setRouteTypeId(LookupHandler.lookupId("route_type_d", "route_type_display", itemState.getRouteType()));
            itemState.getManifested().id = itemState.getManifested().value.equalsIgnoreCase("N/A") ? 1 : 2;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        itemState.setStateDateTimeLocal(itemState.getStateDateTimeLocal().replace("Z", "").replace("T", " "));

        Values output = new Values();
        output.add(itemState.getReference());
        output.add(itemState.getStateDateTimeLocal());
        output.add(itemState.getStateDateId());
        output.add(itemState.getStateTimeId());
        output.add(itemState.getMessageRef());

        /* Gotta get the ItemID somehow.. */
        output.add(itemState.getItemId());

        output.add(itemState.getListId());
        output.add(itemState.getListRef());
        output.add(itemState.getItemClass().id);
        output.add(itemState.getItemStateClass().id);
        output.add(itemState.getResourceId());
        output.add(itemState.getScheduleId());
        output.add(itemState.getNetworkId());
        output.add(itemState.getGeographyId());
        output.add(itemState.getStateCounter());
        output.add(itemState.getBeginDateId());
        output.add(itemState.getEtaStartDate());
        output.add(itemState.getEtaEndDate());
        output.add(itemState.getAdditionalInfo());
        output.add(itemState.getStatusId());
        output.add(itemState.getManifested().id);
        output.add(itemState.getTrackingPoint().id);
        output.add(itemState.getRouteTypeId());
        output.add(itemState.getFromShopId());
        output.add(itemState.getToShopId());
        output.add(itemState.getBillingRef());

        return output;
    }
}
