package Storm.Transformers;

import Storm.AMQPHandler.JSONObjects.Drop;
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
public class DropTransformBolt implements IRichBolt {
    private static final Logger log = LoggerFactory.getLogger(DropTransformBolt.class);

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
        Drop drop = (Drop) tuple.getValueByField("drop");

        try {
            emitValues = transformer.transformDrop(drop);
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
        outputFieldsDeclarer.declareStream(Streams.DROP.id(), Storm.DatabaseHandler.DBObjects.Drop.fields());
        outputFieldsDeclarer.declareStream(Streams.ERROR.id(), new Fields("error_msg"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
