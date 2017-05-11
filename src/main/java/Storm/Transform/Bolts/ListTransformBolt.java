package Storm.Transform.Bolts;

import Storm.AMQPHandler.JSONObjects.ListObj;
import Storm.Transform.ListTransformer;
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

import java.util.Map;

/**
 * Created by charlie on 17/02/17.
 */
public class ListTransformBolt implements IRichBolt {
    private static final Logger log = LoggerFactory.getLogger(ListTransformBolt.class);


    TopologyContext context;
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this._collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {

        ListObj list = (ListObj) tuple.getValueByField("list");
        Transformer<ListObj> transformer = new ListTransformer();
        Values emitValues;

        log.info(String.format("[LOG-LIST] Transforming %s \n Stream ID %s", tuple.getMessageId().toString(), tuple.getSourceStreamId()));

        try {
            emitValues = transformer.transform(list);
            _collector.emit(Streams.LIST.id(), tuple, emitValues);
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
        outputFieldsDeclarer.declareStream(Streams.LIST.id(), Storm.DatabaseHandler.DBObjects.ListObj.fields());
        outputFieldsDeclarer.declareStream(Streams.ERROR.id(), new Fields("error_msg"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
