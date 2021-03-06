package Storm.Transform.Bolts;

import Storm.AMQPHandler.JSONObjects.ItemState;
import Storm.Transform.ItemStateTransformer;
import Storm.Transform.Transformer;
import Storm.Util.Streams;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
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
        Transformer<ItemState> transformer = new ItemStateTransformer();
        Values emitValues;
        ItemState state;

        log.info(String.format("[LOG-ITEM STATE] Transforming %s \n Stream ID %s", tuple.getMessageId().toString(), tuple.getSourceStreamId()));

        if (tuple.getSourceStreamId().equalsIgnoreCase(Streams.ITEM.id())) {
            state = createItemStateCreatedObject(tuple);
        } else {
            state = (ItemState) tuple.getValueByField("item-state");
        }

        try {
            emitValues = transformer.transform(state);
            _collector.emit(Streams.ITEM_STATE.id(), tuple, emitValues);
            _collector.ack(tuple);
        } catch (SQLException | ClassNotFoundException e) {
            if (e.getCause() != null)
                _collector.emit(Streams.ERROR.id(), tuple, new Values(e.getCause().getMessage()));
            else
                _collector.emit(Streams.ERROR.id(), tuple, new Values(e.getMessage()));
        }

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
        state.getOutcomeClass().value = "N/A";
        state.getOutcomeSubClass().value = "N/A";
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
        outputFieldsDeclarer.declareStream(Streams.ITEM_STATE.id(), Storm.DatabaseHandler.DBObjects.ItemState.fields());
        outputFieldsDeclarer.declareStream(Streams.ERROR.id(), new Fields("error_msg"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
