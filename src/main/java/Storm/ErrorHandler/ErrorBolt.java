package Storm.ErrorHandler;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by charlie on 30/01/17.
 */
public class ErrorBolt implements IRichBolt {
    TopologyContext context;
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        context = topologyContext;
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println(String.format("Caught error with tuple. [Error Msg]: %s. Logged error in DB and ack'd offending tuple.",
                tuple.getStringByField("error_msg")));
        _collector.ack(tuple);
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
