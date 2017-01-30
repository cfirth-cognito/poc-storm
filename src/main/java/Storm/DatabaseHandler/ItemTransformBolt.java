package Storm.DatabaseHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

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
        Transformer transformer = new Transformer();
        Values emitValues = new Values();
        String type = tuple.getStringByField("type");

        emitValues.add(type);

        // Can't do this atm. See @declareOutputFields
//        switch (type) {
//            case "item":
//                Item item = (Item) tuple.getValueByField("item");
//                emitValues.add(transformer.transformItem(item));
//                break;
//        }

        Item item = (Item) tuple.getValueByField("item");
        emitValues.add(transformer.transformItem(item));

        System.out.println("[LOG] JSON transformed, emitting..");
        outputCollector.emit(tuple, emitValues);
        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    // hmm
    // this needs to match the columns we're inserting in the persist bolt, so we can't use (col1, col2) etc to make it generic
    // ..unless we implement a custom JDBCMapper?
    // also different messages will have a different number of fields to insert
    // This also gets set when the topology is created, so we can't modify it dynamically depending on the msg we receive in the tuple
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
