package Storm.Transformers;

import Storm.AMQPHandler.JSONObjects.Item;
import Storm.AMQPHandler.JSONObjects.ItemState;
import Storm.AMQPHandler.JSONObjects.ListObj;
import Storm.DatabaseHandler.LookupHandler;
import com.google.common.collect.Lists;
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

    private static int DATE_LENGTH = 8;

    Values transformItem(Item item) {
        try {
            Map<String, String> columns = new TreeMap<>();
            columns.put("class_display", "String");
            columns.put("subclass_display", "String");
//            item.setItemClassDisplay(String.valueOf(LookupHandler.lookupId("inv_item_class_type_d", "class_display", item.getItemClass())));
            List<Object> returned = LookupHandler.customLookUp("select class_display, subclass_display from inv_item_class_type_d where class = '" + item.getItemClass() + "'" +
                    " AND subclass = '" + item.getItemSubClass() + "'", columns);
            if (!returned.isEmpty()) {
                item.setItemClassDisplay((String) returned.get(0));
                item.setItemSubClassDisplay((String) returned.get(1));
            } else {
                item.setItemClassDisplay("Unknown");
                item.setItemSubClassDisplay("Unknown");
            }
//            item.setItemSubClassDisplay(String.valueOf(LookupHandler.lookupId("inv_item_class_type_d", "subclass_display", item.getItemSubClass())));
            item.setClientId(LookupHandler.lookupId("clients_d", "client_code", item.getClient()));
            item.setScheduleId(LookupHandler.getScheduleId(item.getRouteType(), item.getRouteRef()));

            columns.clear();
            columns.put("class_display", "String");
            List<Object> statusDisplay = LookupHandler.customLookUp("SELECT class_display FROM inv_item_status_type_d WHERE class = '" + item.getStatus().value + "'", columns);
            if (!statusDisplay.isEmpty())
                item.getStatusDisplay().value = (String) statusDisplay.get(0);
            else {
                String status = item.getStatusDisplay().value;
                String[] tempStatus = status.split("_");
                if (tempStatus.length > 0) {
                    for (int i = 0; i < tempStatus.length; i++) {
                        tempStatus[i] = tempStatus[i] + tempStatus[i].substring(1).toLowerCase();
                    }
                }
                status = tempStatus[0] + " " + tempStatus[1];
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

    Values transformItemState(ItemState itemState) {
        try {
            log.info("Transforming ItemState. ItemID: " + itemState.getItemId());
            if (itemState.getItemId() == null) {
                TreeMap<String, String> columns = new TreeMap<>(); // TreeMap to ensure ordered results
                columns.put("id", "Integer");
                columns.put("inv_item_class", "String");
                columns.put("inv_item_subclass", "String");
                columns.put("inv_item_status", "String");
                log.info("Looking up Item Dimension");

                java.util.List<Object> itemDimension = LookupHandler.lookupDimension("inv_item_d", columns, itemState.getReference(), "inv_item_ref");
                if (itemDimension != null && !itemDimension.isEmpty()) {
                    itemState.setItemId((Integer) itemDimension.get(0));
                    itemState.getItemClass().value = (String) itemDimension.get(1);
                    itemState.setStatus((String) itemDimension.get(2));
                    itemState.getItemSubClass().value = ((String) itemDimension.get(3));

                } else {
                    log.info(String.format("No Item Dimension found for %s", itemState.getReference()));
                    itemState.setItemId(1);
                }
            }

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

            /* Lookup ListObj */
            TreeMap<String, String> columns = new TreeMap<>();
            columns.put("id", "Integer");
            columns.put("begin_date", "Date");
            columns.put("begin_date_id", "Integer");

            List<Object> listDimension = LookupHandler.lookupDimension("inv_list_d", columns, itemState.getListRef(), "inv_list_ref");
            if (listDimension != null && !listDimension.isEmpty()) {
                itemState.setBeginDate((String) listDimension.get(0));
                itemState.setBeginDateId((Integer) listDimension.get(1));
                itemState.setListId((Integer) listDimension.get(2));
            } else {
                itemState.setListId(1);

                /* Special BeginDate handling. THERE MUST ALWAYS BE A BEGIN DATE ASSOCIATED WITH A STATE. */
                columns = new TreeMap<>();
                columns.put("begin_date_id", "Integer");
                List<Object> lookup = LookupHandler.customLookUp(
                        "SELECT MAX(begin_date_id) AS begin_date_id FROM inv_item_state_f WHERE inv_item_ref = '"
                                + itemState.getReference() + "';", columns);
                itemState.setBeginDateId((lookup.isEmpty() || (int) lookup.get(0) == 0) ? itemState.getStateDateId() : (Integer) lookup.get(0));
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        System.out.println(itemState.getStateDateTimeLocal());
        itemState.setStateDateTimeLocal(transformDate(itemState.getStateDateTimeLocal()));
        itemState.setEtaStartDate(transformDate(itemState.getEtaStartDate()));
        itemState.setEtaEndDate(transformDate(itemState.getEtaEndDate()));

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

//    Values transformList(ListObj listObj) {
//        try {
//
//            listObj.getListClass().id = LookupHandler.lookupId("inv_list_class_type_d", Lists.asList("id", new String[]{"class_display", "subclass_display",
//                    "sort_order", "subclass"}), Lists.asList(listObj.getListClass().value, new String[]{}));
//            listObj.setItemSubClassDisplay(String.valueOf(LookupHandler.lookupId("inv_item_class_type_d", "subclass_display", listObj.getItemSubClass())));
//            listObj.setClientId(LookupHandler.lookupId("clients_d", "client_code", listObj.getClient()));
//            listObj.setScheduleId(LookupHandler.getScheduleId(listObj.getRouteType(), listObj.getRouteRef()));
//
//        } catch (ClassNotFoundException | SQLException e) {
//            e.printStackTrace();
//        }
//
//        listObj.setEventDate(listObj.getEventDate().replace("Z", "").replace("T", " "));
//
//        Values output = new Values();
//        output.add(listObj.getReference());
//        output.add(listObj.getItemClass());
//        output.add(listObj.getItemSubClass());
//        output.add(listObj.getStatus());
//        output.add(listObj.getItemClassDisplay());
//        output.add(listObj.getItemSubClassDisplay());
//        output.add(listObj.getStatus());
//        output.add(listObj.getReference());
//        output.add(listObj.getStatedDay());
//        output.add(listObj.getStatedTime());
//        output.add(listObj.getClient());
//        output.add(listObj.getCustomerName());
//        output.add(listObj.getCustAddr());
//        output.add(1);
//        output.add(listObj.getEventDate());
//        output.add(listObj.getScheduleId());
//        output.add(listObj.getPostcode());
//        output.add(listObj.getClientId());
//        output.add(listObj.getRouteType());
//
//        // return item as listObj of fields
//        return output;
//    }

    String transformDate(String date) {
        System.out.println(date);

        if (date == null) return null;

        if (date.contains("."))
            return date.substring(0, date.lastIndexOf(".") + 3);
        else if (date.contains("Z"))
            return date.replace("Z", "").replace("T", " ");
        else
            return null;


        //            if (itemState.getStateDateTimeLocal().contains("+"))
//                itemState.setStateDateTimeLocal(itemState.getStateDateTimeLocal().substring(0, itemState.getStateDateTimeLocal().lastIndexOf("+") - 1));
//        }

        //2017-02-14T17:22:16.078Z
//        if (!itemState.getStateDateTimeLocal().contains("+") && !itemState.getStateDateTimeLocal().contains("Z"))
    }

    private String transformStatedDay(String day) {
        String statedDay;
        switch (day) {
            case "1":
                statedDay = "MON";
                break;
            case "2":
                statedDay = "TUE";
                break;
            case "3":
                statedDay = "WED";
                break;
            case "4":
                statedDay = "THU";
                break;
            case "5":
                statedDay = "FRI";
                break;
            case "6":
                statedDay = "SAT";
                break;
            case "7":
                statedDay = "SUN";
                break;
            default:
                statedDay = null;
        }
        return statedDay;
    }

    private String transformStatedTime(String time) {
        int iTime = Integer.parseInt(time);
        return (iTime >= 0 && iTime < 12) ? "AM" : "PM";
    }
}
