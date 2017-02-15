package Storm.DatabaseHandler;

import Storm.AMQPHandler.JSONObj.Item.ItemState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by charlie on 30/01/17.
 */
public class ItemStateTransformBolt implements IRichBolt {
    private static final Logger log = LoggerFactory.getLogger(ItemStateTransformBolt.class);

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
        log.info(String.format("[LOG] ItemState Transforming %s \n Stream ID %s", tuple.getMessageId().toString(), tuple.getSourceStreamId()));
        ItemState state;
        if (tuple.getSourceStreamId().equalsIgnoreCase("item")) {
            state = createItemStateCreatedObject(tuple);
        } else {
            state = (ItemState) tuple.getValueByField("item-state");
        }

        emitValues = transformer.transformItemState(state);
        _collector.emit("item-state", tuple, emitValues);
        _collector.ack(tuple);
    }

    private ItemState createItemStateCreatedObject(Tuple tuple) {
        ItemState state = new ItemState();
        Tuple item = (Tuple) tuple.getValueByField("values");

        state.setItemId(tuple.getIntegerByField("id"));
        state.setScheduleId(item.getIntegerByField("schedule_mgmt_id"));
        state.getItemClass().value = item.getStringByField("inv_item_class");
        state.getItemSubClass().value = item.getStringByField("inv_item_subclass");
        state.setReference(item.getStringByField("inv_item_ref"));
        state.setStateDateTimeLocal(item.getStringByField("event_date"));
        state.getItemStateClass().value = "CREATED";
        state.getManifested().value = "N/A";
        state.getItemStateSubClass().value = "N/A";
        state.setResourceId(1);
        state.setListId(1);
        state.setNetworkId(1);
        state.setGeographyId(1);
        state.setStateCounter(1);
        state.setBeginDateId(1);
        state.setListRef("N/A");
        state.setMessageRef("N/A");
        state.setAdditionalInfo("");
        state.setRouteType(item.getStringByField("route_type"));
        return state;
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("item-state", Storm.DatabaseHandler.DBObjects.ItemState.fields());
        outputFieldsDeclarer.declareStream("ErrorStream", new Fields("error_msg"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
