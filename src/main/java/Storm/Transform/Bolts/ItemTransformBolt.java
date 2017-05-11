package Storm.Transform.Bolts;

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
import Storm.Transform.*;

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
        Item item = (Item) tuple.getValueByField("item");
        Transformer<Item> transformer = new ItemTransformer();
        Values emitValues;

        log.info(String.format("[LOG-ITEM] Transforming %s \n Stream ID %s", tuple.getMessageId().toString(), tuple.getSourceStreamId()));

        try {
            emitValues = transformer.transform(item);
            _collector.emit(Streams.ITEM.id(), tuple, emitValues);
            _collector.ack(tuple);
        } catch (Exception e) {
            if (e.getCause() != null)
                _collector.emit(Streams.ERROR.id(), tuple, new Values(e.getCause().getMessage()));
            else
                _collector.emit(Streams.ERROR.id(), tuple, new Values(e.getMessage()));
        }


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
