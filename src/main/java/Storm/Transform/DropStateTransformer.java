package Storm.Transform;

import Storm.AMQPHandler.JSONObjects.DropState;
import Storm.DatabaseHandler.LookupHandler;
import Storm.Transform.Bolts.ItemTransformBolt;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by charlie on 22/03/17.
 */
public class DropStateTransformer extends Transformer<DropState> {
    private static final Logger log = LoggerFactory.getLogger(DropStateTransformer.class);

    @Override
    public Values transform(DropState dropState) throws SQLException, ClassNotFoundException {
        try {
            log.info("Transforming ItemState. ItemID: " + dropState.getItemId());
            if (dropState.getDropID() == null) {
                TreeMap<String, String> columns = new TreeMap<>(); // TreeMap to ensure ordered results
                columns.put("id", "Integer");
                columns.put("drop_class", "String");
                columns.put("drop_subclass", "String");
                columns.put("drop_status", "String");
                log.info("Looking up Item Dimension");

                List<Object> dropDimension = LookupHandler.lookupDimension("drop_d", columns, dropState.getReference(), "drop_ref");
                if (dropDimension != null && !dropDimension.isEmpty()) {
                    dropState.setDropID((Integer) dropDimension.get(0));
                    dropState.getDropClass().value = (String) dropDimension.get(1);
                    dropState.getDropStatus().value = (String) dropDimension.get(2);
                    dropState.getDropSubClass().value = ((String) dropDimension.get(3));

                } else {
                    log.info(String.format("No Drop Dimension found for %s", dropState.getReference()));
                    dropState.setDropID(1);
                }
            }

            dropState.getDropClass().id = LookupHandler.lookupId("inv_item_class_type_d", Arrays.asList("class", "subclass")
                    , Arrays.asList(dropState.getDropClass().value, dropState.getDropSubClass().value));
            dropState.getDropStateClass().id = LookupHandler.lookupId("inv_item_state_type_d", Arrays.asList("class", "subclass", "outcome_class", "outcome_subclass"),
                    Arrays.asList(dropState.getDropStateClass().value, dropState.getDropStateSubClass().value, dropState.getOutcomeClass().value, dropState.getOutcomeSubClass().value));

            /* Handle the mobile sending in unsupported outcomes, but with a valid class & subclass combination */
            if (dropState.getDropStateClass().id == 1)
                dropState.getDropStateClass().id = LookupHandler.lookupId("inv_item_state_type_d", Arrays.asList("class", "subclass", "outcome_class", "outcome_subclass"),
                        Arrays.asList(dropState.getDropStateClass().value, dropState.getDropStateSubClass().value, "N/A", "N/A"));

            dropState.getDropStatus().id = LookupHandler.lookupId("inv_item_status_type_d", "class", dropState.getDropStatus().value);

            ArrayList<Integer> dateTimeIds = LookupHandler.lookUpDateTime(dropState.getEventDate());
            dropState.setStateDateId(dateTimeIds.get(0));
            dropState.setStateTimeId(dateTimeIds.get(1));

            dropState.setGeographyId(0);
            dropState.setNetworkId(0);
            dropState.setStateCounter(1);

            dropState.getRouteType().id = LookupHandler.lookupId("route_type_d", "route_type_display", dropState.getRouteType().value);
            dropState.getManifested().id = (dropState.getList().value == null || dropState.getList().value.equalsIgnoreCase("N/A")) ? 1 : 2;

            dropState.getTrackingPoint().id = LookupHandler.lookupId("tracking_points_d", "tracking_point_code", dropState.getTrackingPoint().value);
            dropState.getFromShop().id = LookupHandler.lookupId("schedule_management_dh", "parcelshop_tier5", dropState.getFromShop().value);
            dropState.getToShop().id = LookupHandler.lookupId("schedule_management_dh", "parcelshop_tier5", dropState.getToShop().value);

            dropState.setScheduleId(LookupHandler.getScheduleId(dropState.getRouteType(), dropState.getRouteRef()));
            dropState.setResourceId(LookupHandler.lookupId("resource_management_dh", "resource_ref", dropState.getResourceRef()));

            /* Lookup ListObj */
            TreeMap<String, String> columns = new TreeMap<>();
            columns.put("id", "Integer");
            columns.put("begin_date", "Date");
            columns.put("begin_date_id", "Integer");

            List<Object> listDimension = LookupHandler.lookupDimension("inv_list_d", columns, dropState.getListRef(), "inv_list_ref");
            if (listDimension != null && !listDimension.isEmpty()) {
                dropState.setBeginDate((String) listDimension.get(0));
                dropState.setBeginDateId((Integer) listDimension.get(1));
                dropState.setListId((Integer) listDimension.get(2));
            } else {
                dropState.setListId(1);

                /* Special BeginDate handling. THERE MUST ALWAYS BE A BEGIN DATE ASSOCIATED WITH A STATE. */
                columns = new TreeMap<>();
                columns.put("begin_date_id", "Integer");
                List<Object> lookup = LookupHandler.customLookUp(
                        "SELECT MAX(begin_date_id) AS begin_date_id FROM inv_item_state_f WHERE inv_item_ref = '"
                                + dropState.getReference() + "';", columns);
                dropState.setBeginDateId((lookup.isEmpty() || (int) lookup.get(0) == 0) ? dropState.getStateDateId() : (Integer) lookup.get(0));
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        System.out.println(dropState.getStateDateTimeLocal());
        dropState.setStateDateTimeLocal(transformDate(dropState.getStateDateTimeLocal()));
        dropState.setEtaStartDate(transformDate(dropState.getEtaStartDate()));
        dropState.setEtaEndDate(transformDate(dropState.getEtaEndDate()));

        Values output = new Values();
        output.add(dropState.getReference());
        output.add(dropState.getStateDateTimeLocal());
        output.add(dropState.getStateDateId());
        output.add(dropState.getStateTimeId());
        output.add(dropState.getMessageRef());


        output.add(dropState.getItemId());

        output.add(dropState.getListId());
        output.add(dropState.getListRef());
        output.add(dropState.getItemClass().id);
        output.add(dropState.getItemStateClass().id);
        output.add(dropState.getResourceId());
        output.add(dropState.getScheduleId());
        output.add(dropState.getNetworkId());
        output.add(dropState.getGeographyId());
        output.add(dropState.getStateCounter());
        output.add(dropState.getBeginDateId());
        output.add(dropState.getEtaStartDate());
        output.add(dropState.getEtaEndDate());
        output.add(dropState.getAdditionalInfo());
        output.add(dropState.getStatusId());
        output.add(dropState.getManifested().id);
        output.add(dropState.getTrackingPoint().id);
        output.add(dropState.getRouteTypeId());
        output.add(dropState.getFromShop().id);
        output.add(dropState.getToShop().id);
        output.add(dropState.getBillingRef());

        return output;
    }
}
