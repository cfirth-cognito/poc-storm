package Storm.Transform;

import org.apache.storm.tuple.Values;

import java.sql.SQLException;

/**
 * Created by charlie on 21/03/17.
 */
public class ListTransformer<Obj> extends Transformer<Obj> {

    @Override
    public Values transform(Obj obj) throws SQLException, ClassNotFoundException {
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
        return null;
    }
}
