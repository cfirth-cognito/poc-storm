package Storm.Transform;

import Storm.AMQPHandler.JSONObjects.ListObj;
import Storm.DatabaseHandler.LookupHandler;
import org.apache.storm.tuple.Values;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by charlie on 21/03/17.
 */
public class ListTransformer extends Transformer<ListObj> {

    @Override
    public Values transform(ListObj listObj) throws SQLException, ClassNotFoundException {

        listObj.setEventDate(transformDate(listObj.getEventDate()));
        listObj.getBeginDate().value = transformDate(listObj.getBeginDate().value);
        listObj.getSchedule().id = LookupHandler.getScheduleId(listObj.getRouteType().value, listObj.getSchedule().value);


        Map<String, String> columns = new TreeMap<>();
        columns.put("id", "String");
        columns.put("class_display", "String");
        columns.put("subclass_display", "String");
        columns.put("sort_order", "Integer");
        columns.put("subclass", "String");
        List<Object> lookupResults = LookupHandler.customLookUp(
                String.format("select %s " +
                        "FROM inv_list_class_type_d " +
                        "WHERE class = %s", columns.keySet().toString(), listObj.getListClass().value), columns);

        if (lookupResults != null && !lookupResults.isEmpty()) {
            listObj.getListClass().id = (int) lookupResults.get(0);
            listObj.setListClassDisplay((String) lookupResults.get(1));
            listObj.setListSubclassClassDisplay((String) lookupResults.get(2));
            listObj.setSortOrder(Integer.valueOf((String) lookupResults.get(3)));
            listObj.getListSubClass().value = (String) lookupResults.get(4);
        } else {
            listObj.getListClass().id = 1;
            listObj.setListClassDisplay("Unknown");
            listObj.setListSubclassClassDisplay("Unknown");
            listObj.setSortOrder(1);
            listObj.getListSubClass().value = "UNKNOWN";
        }

        listObj.getBeginDate().id = LookupHandler.lookupId("date_d", "full_date", listObj.getBeginDate().value);

        Values output = new Values();
        output.add(listObj.getReference());
        output.add(listObj.getListClass().value);
        output.add(listObj.getListClassDisplay());
        output.add(listObj.getSortOrder());
        output.add(listObj.getEventDate());
        output.add(listObj.getBeginDate().value);
        output.add(listObj.getSchedule().id);
        output.add(listObj.getBeginDate().id);
        output.add(listObj.getRouteType().value);

        return output;
    }
}
