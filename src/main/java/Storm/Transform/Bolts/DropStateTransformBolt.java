package Storm.Transform.Bolts;

import Storm.AMQPHandler.JSONObjects.DropState;
import Storm.Transform.DropStateTransformer;
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
public class DropStateTransformBolt implements IRichBolt {
    private static final Logger log = LoggerFactory.getLogger(DropStateTransformBolt.class);

    TopologyContext context;
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this._collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        log.info(String.format("[LOG-DROP STATE] Transforming %s \n Stream ID %s", tuple.getMessageId().toString(), tuple.getSourceStreamId()));

        Transformer<DropState> transformer = new DropStateTransformer();
        Values emitValues;
        DropState state = (DropState) tuple.getValueByField("drop-state");

        try {
            emitValues = transformer.transform(state);
            _collector.emit(Streams.DROP_STATE.id(), tuple, emitValues);
            _collector.ack(tuple);
        } catch (SQLException | ClassNotFoundException e) {
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
        outputFieldsDeclarer.declareStream(Streams.DROP_STATE.id(), Storm.DatabaseHandler.DBObjects.DropState.fields());
        outputFieldsDeclarer.declareStream(Streams.ERROR.id(), new Fields("error_msg"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
