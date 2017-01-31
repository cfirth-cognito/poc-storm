package Storm.DatabaseHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */
public class ItemTransformBolt implements IRichBolt {

    TopologyContext context;
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this._collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        Transformer transformer = new Transformer();
        Values emitValues;
        System.out.println(tuple.getValues());
        System.out.println(String.format("Transforming %s", tuple.getMessageId().toString()));

        Item item = (Item) tuple.getValueByField("item");
        emitValues = transformer.transformItem(item);

        System.out.println("[LOG] Item Object transformed, emitting..");
        _collector.emit("item", tuple, emitValues);
        _collector.ack(tuple);
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
        outputFieldsDeclarer.declareStream("item", Storm.DatabaseHandler.DBObjects.Item.fields());
        outputFieldsDeclarer.declareStream("ErrorStream", new Fields("error_msg"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
