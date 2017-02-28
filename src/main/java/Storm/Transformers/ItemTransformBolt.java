package Storm.Transformers;

import Storm.AMQPHandler.JSONObjects.Item;
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

import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */
public class ItemTransformBolt implements IRichBolt {
    private static final Logger log = LoggerFactory.getLogger(ItemTransformBolt.class);

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
        log.info(String.format("Transforming %s", tuple.getMessageId().toString()));

        Item item = (Item) tuple.getValueByField("item");
        emitValues = transformer.transformItem(item);

        _collector.emit(Streams.ITEM.id(), tuple, emitValues);
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.ITEM.id(), Storm.DatabaseHandler.DBObjects.Item.fields());
        outputFieldsDeclarer.declareStream(Streams.ERROR.id(), new Fields("error_msg"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
