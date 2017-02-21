package Storm.Util;

import Storm.AMQPHandler.JSONObjects.ItemState;
import Storm.DatabaseHandler.LookupHandler;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Charlie on 21/02/2017.
 */
public class SequencingBolt implements IRichBolt {

    TopologyContext context;
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        switch (tuple.getSourceStreamId()) {
            case "item":
                /* Goto Cassandra. Check for item states. Emit them */
                List<ItemState> states = new ArrayList<>();
                for (ItemState state : states)
                    _collector.emit("item-state", tuple, new Values(state));
                break;
            case "item-state":
                /* Check for Item */
                try {
                    ItemState itemState = (ItemState) tuple.getValueByField("item-state");
                    int id = LookupHandler.lookupId("inv_item_d", "inv_item_ref", itemState.getReference());
                    if (id > 1)
                        _collector.emit("item-state", tuple, new Values(itemState));
                    else
                        _collector.fail(tuple); // No item yet.

                } catch (Exception e) {

                }
                break;
        }

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
