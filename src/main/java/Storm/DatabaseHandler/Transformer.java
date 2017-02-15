package Storm.DatabaseHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import Storm.AMQPHandler.JSONObj.Item.ItemState;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

/**
 * Created by charlie on 30/01/17.
 */
public class Transformer {
    private static final Logger log = LoggerFactory.getLogger(Transformer.class);


    Values transformItem(Item item) {
        try {

            item.setItemClassDisplay(String.valueOf(LookupHandler.lookupId("inv_item_class_type_d", "class_display", item.getItemClass())));
            item.setItemSubClassDisplay(String.valueOf(LookupHandler.lookupId("inv_item_class_type_d", "subclass_display", item.getItemSubClass())));
            item.setClientId(LookupHandler.lookupId("clients_d", "client_code", item.getClient()));
            item.setScheduleId(LookupHandler.getScheduleId(item.getRouteType(), item.getRouteRef()));

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
        try {
            if (itemState.getItemId() == null)
                itemState.setItemId(LookupHandler.lookupId("inv_item_d", "inv_item_ref", itemState.getReference()));

            itemState.getItemClass().id = LookupHandler.lookupId("inv_item_class_type_d", Arrays.asList("class", "subclass")
                    , Arrays.asList(itemState.getItemClass().value, itemState.getItemSubClass().value));
            itemState.getItemStateClass().id = LookupHandler.lookupId("inv_item_state_type_d", Arrays.asList("class", "subclass"),
                    Arrays.asList(itemState.getItemStateClass().value, itemState.getItemStateSubClass().value));
            itemState.setStatusId(LookupHandler.lookupId("inv_item_status_type_d", "class", itemState.getStatus()));

            ArrayList<Integer> dateTimeIds = LookupHandler.lookUpDateTime(itemState.getStateDateTimeLocal());
            itemState.setStateDateId(dateTimeIds.get(0));
            itemState.setStateTimeId(dateTimeIds.get(1));

            itemState.setGeographyId(0);
            itemState.setNetworkId(0);
            itemState.setStateCounter(1);

            itemState.setRouteTypeId(LookupHandler.lookupId("route_type_d", "route_type_display", itemState.getRouteType()));
            itemState.getManifested().id = (itemState.getListRef() == null || itemState.getListRef().equalsIgnoreCase("N/A")) ? 1 : 2;

            itemState.getTrackingPoint().id = LookupHandler.lookupId("tracking_point_d", "tracking_point_code", itemState.getTrackingPoint().value);
            itemState.getFromShop().id = LookupHandler.lookupId("schedule_management_dh", "parcelshop_tier5", itemState.getFromShop().value);
            itemState.getToShop().id = LookupHandler.lookupId("schedule_management_dh", "parcelshop_tier5", itemState.getToShop().value);

            itemState.setScheduleId(LookupHandler.getScheduleId(itemState.getRouteType(), itemState.getRouteRef()));
            itemState.setResourceId(LookupHandler.lookupId("resource_management_dh", "resource_ref", itemState.getResourceRef()));

            /* Lookup List */
            Map<String, String> columns = new HashMap<>();
            columns.put("id", "Integer");
            columns.put("begin_date", "Date");
            columns.put("begin_date_id", "Integer");

            List<Object> listDimension = LookupHandler.lookupDimension("inv_list_d", columns, itemState.getListRef(), "inv_list_ref");
            if (listDimension != null) {
                itemState.setListId((Integer) listDimension.get(0));
                itemState.setBeginDate((String) listDimension.get(1));
                itemState.setBeginDateId((Integer) listDimension.get(2));
            } else {
                itemState.setListId(1);

                /* Special BeginDate handling. THERE MUST ALWAYS BE A BEGIN DATE ASSOCIATED WITH A STATE. */
                columns = new HashMap<>();
                columns.put("begin_date_id", "Integer");
                List<Object> lookup = LookupHandler.customLookUp(
                        "SELECT MAX(begin_date_id) AS begin_date_id FROM inv_item_state_f WHERE inv_item_ref = '"
                                + itemState.getReference() + "';", columns);
                itemState.setBeginDateId((lookup.isEmpty()) ? itemState.getStateDateId() : (Integer) lookup.get(0));
            }


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
        output.add(itemState.getFromShop().id);
        output.add(itemState.getToShop().id);
        output.add(itemState.getBillingRef());

        return output;
    }
}
