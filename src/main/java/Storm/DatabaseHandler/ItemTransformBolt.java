package Storm.DatabaseHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */
public class ItemTransformBolt implements IRichBolt {

    TopologyContext context;
    OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        // Read in a item obj
        Item item = (Item) tuple.getValueByField("item");
        // lookup needed fields

        System.out.println("CF Transforming Item now..");

        try {
            item.setItemClass(String.valueOf(LookupHandler.lookupId("inv_item_class_type_d", "class", item.getItemClass())));
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        // factor out logic into seperate java class - this class then does the lookups?
        // should I use storms lookup bolt? or do it manually?
        // return item obj with transformed fields

        List<Object> output = new ArrayList<>();
        output.add(item.getReference());
        output.add(item.getItemClass());
        output.add(item.getItemSubClass());
        output.add(item.getStatus());
        output.add(item.getItemClass());
        output.add(item.getItemSubClass());
        output.add(item.getStatus());
        output.add(item.getReference());
        output.add(item.getStatedDay());
        output.add(item.getStatedTime());
        output.add(item.getClient());
        output.add(item.getCustomerName());
        output.add(item.getCustAddr());
        output.add(1);
        output.add(item.getEventDate());
        output.add(item.getPostcode());
        output.add(item.getRouteType());

        System.out.println("CF item transformed, emitting..");
        outputCollector.emit(tuple, output);
        // then persist item obj to db
        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("inv_item_ref", "inv_item_class", "inv_item_subclass", "inv_item_status", "inv_item_class_display",
                "inv_item_subclass_display", "inv_item_status_display", "barcode", "stated_day"
                , "stated_time", "client", "customer_name", "customer_address_1", "version", "event_date", "postcode", "route_type"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
