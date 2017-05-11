package Storm.Transform;

import Storm.AMQPHandler.JSONObjects.DropState;
import Storm.DatabaseHandler.LookupHandler;
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

        TreeMap<String, String> columns = new TreeMap<>(); // TreeMap to ensure ordered results
        columns.put("id", "Integer");
        columns.put("drop_class", "String");
        columns.put("drop_subclass", "String");
        columns.put("drop_status", "String");

        log.info("Looking up Drop Dimension");
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

        dropState.getDropClass().id = LookupHandler.lookupId("drop_class_type_d", Arrays.asList("class", "subclass")
                , Arrays.asList(dropState.getDropClass().value, dropState.getDropSubClass().value));
        dropState.getDropStateClass().id = LookupHandler.lookupId("drop_state_type_d", Arrays.asList("class", "subclass", "outcome_class", "outcome_subclass"),
                Arrays.asList(dropState.getDropStateClass().value, dropState.getDropStateSubClass().value, dropState.getOutcomeClass().value, dropState.getOutcomeSubClass().value));

            /* Handle the mobile sending in unsupported outcomes, but with a valid class & subclass combination */
        if (dropState.getDropStateClass().id == 1)
            dropState.getDropStateClass().id = LookupHandler.lookupId("drop_state_type_d", Arrays.asList("class", "subclass", "outcome_class", "outcome_subclass"),
                    Arrays.asList(dropState.getDropStateClass().value, dropState.getDropStateSubClass().value, "N/A", "N/A"));

        dropState.getDropStatus().id = LookupHandler.lookupId("drop_status_type_d", "class", dropState.getDropStatus().value);

        ArrayList<Integer> dateTimeIds = LookupHandler.lookUpDateTime(dropState.getEventDate());
        dropState.setStateDateId(dateTimeIds.get(0));
        dropState.setStateTimeId(dateTimeIds.get(1));

        dropState.setGeographyId(0);
        dropState.setNetworkId(0);
        dropState.setStateCounter(1);

        dropState.getRouteType().id = LookupHandler.lookupId("route_type_d", "route_type_display", dropState.getRouteType().value);
        dropState.getManifested().id = (dropState.getList().value == null || dropState.getList().value.equalsIgnoreCase("N/A")) ? 1 : 2;

        dropState.getTrackingPoint().id = LookupHandler.lookupId("tracking_points_d", "tracking_point_code", dropState.getTrackingPoint().value);

        dropState.getSchedule().id = LookupHandler.getScheduleId(dropState.getRouteType().value, dropState.getRoute().value);
        dropState.getResource().id = LookupHandler.lookupId("resource_management_dh", "resource_ref", dropState.getResource().value);
        dropState.getShop().id = dropState.getSchedule().id;

            /* Lookup ListObj */
        columns = new TreeMap<>();
        columns.put("id", "Integer");
        columns.put("begin_date_id", "Integer");

        List<Object> listDimension = LookupHandler.lookupDimension("inv_list_d", columns, dropState.getList().value, "inv_list_ref");
        if (listDimension != null && !listDimension.isEmpty()) {
            dropState.getBeginDate().id = (Integer) listDimension.get(0);
            dropState.getList().id = (Integer) listDimension.get(1);
        } else {
            dropState.getList().id = 1;
        }


        dropState.setEventDate(transformDate(dropState.getEventDate()));

        Values output = new Values();
        output.add(dropState.getReference());
        output.add(dropState.getEventDate());
        output.add(dropState.getStateDateId());
        output.add(dropState.getStateTimeId());
        output.add(dropState.getMessageRef());
        output.add(dropState.getDropID());
        output.add(dropState.getList().id);
        output.add(dropState.getList().value);
        output.add(dropState.getDropClass().id);
        output.add(dropState.getDropStateClass().id);
        output.add(dropState.getResource().id);
        output.add(dropState.getSchedule().id);
        output.add(dropState.getNetworkId());
        output.add(dropState.getGeographyId());
        output.add(dropState.getStateCounter());
        output.add(dropState.getBeginDate().id);
        output.add(dropState.getDropStatus().id);
        output.add(dropState.getManifested().id);
        output.add(dropState.getTrackingPoint().id);
        output.add(dropState.getRouteType().id);
        output.add(dropState.getBillingRef());

        output.add(dropState.getShop().id);
        output.add(dropState.getRoute().id);
        output.add(Double.valueOf(dropState.getLat()));
        output.add(Double.valueOf(dropState.getLongitude()));
        output.add(dropState.getValidity());
        output.add(dropState.getDuration());
        output.add(dropState.getCustomerName());
        output.add(dropState.getDesignRank());

        return output;
    }
}
