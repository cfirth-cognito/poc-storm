package Storm.DatabaseHandler;

import Storm.AMQPHandler.JSONObj.Item.ItemState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * Created by charlie on 30/01/17.
 */
public class ItemStateTransformBolt implements IRichBolt {

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
        ItemState state;
        if (tuple.getSourceStreamId().equalsIgnoreCase("item")) {
            state = createItemCreatedStateObject(tuple);
        } else {
            state = (ItemState) tuple;
        }

        emitValues = transformer.transformItemCreatedState(state);

        System.out.println("[LOG] JSON transformed, emitting..");
        _collector.emit(tuple, emitValues);
        _collector.ack(tuple);
    }


    private ItemState createItemCreatedStateObject(Tuple item) {
        ItemState state = new ItemState();
        state.setScheduleId(item.getIntegerByField("schedule_mgmt_id"));
        state.setItemClass(item.getStringByField("inv_item_class"));
        state.setItemStateClass("CREATED");
        state.setItemStateSubClass("N/A");
        state.setResourceId(1);
        state.setListId(1);
        state.setNetworkId(1);
        state.setGeographyId(1);
        state.setStateCounter(1);
        state.setListRef("N/A");
        state.setMessageRef("N/A");
        return state;
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
