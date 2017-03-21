package Storm.Transform;

import Storm.AMQPHandler.JSONObjects.ItemState;
import Storm.DatabaseHandler.LookupHandler;
import Storm.Transform.Transformer;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by charlie on 21/03/17.
 */
public class ItemStateTransformer<Obj> extends Transformer<Obj> {
    private static final Logger log = LoggerFactory.getLogger(ItemStateTransformer.class);

    @Override
    public Values transform(Obj obj) {
        ItemState itemState = (ItemState) obj;
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
            itemState.getItemStateClass().id = LookupHandler.lookupId("inv_item_state_type_d", Arrays.asList("class", "subclass", "outcome_class", "outcome_subclass"),
                    Arrays.asList(itemState.getItemStateClass().value, itemState.getItemStateSubClass().value, itemState.getOutcomeClass().value, itemState.getOutcomeSubClass().value));

            /* Handle the mobile sending in unsupported outcomes, but with a valid class & subclass combination */
            if (itemState.getItemStateClass().id == 1)
                itemState.getItemStateClass().id = LookupHandler.lookupId("inv_item_state_type_d", Arrays.asList("class", "subclass", "outcome_class", "outcome_subclass"),
                        Arrays.asList(itemState.getItemStateClass().value, itemState.getItemStateSubClass().value, "N/A", "N/A"));
            itemState.setStatusId(LookupHandler.lookupId("inv_item_status_type_d", "class", itemState.getStatus()));

            ArrayList<Integer> dateTimeIds = LookupHandler.lookUpDateTime(itemState.getStateDateTimeLocal());
            itemState.setStateDateId(dateTimeIds.get(0));
            itemState.setStateTimeId(dateTimeIds.get(1));

            itemState.setGeographyId(0);
            itemState.setNetworkId(0);
            itemState.setStateCounter(1);

            itemState.setRouteTypeId(LookupHandler.lookupId("route_type_d", "route_type_display", itemState.getRouteType()));
            itemState.getManifested().id = (itemState.getListRef() == null || itemState.getListRef().equalsIgnoreCase("N/A")) ? 1 : 2;

            itemState.getTrackingPoint().id = LookupHandler.lookupId("tracking_points_d", "tracking_point_code", itemState.getTrackingPoint().value);
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
}
