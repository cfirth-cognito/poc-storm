package Storm.AMQPHandler;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */
public class ParseAMQPBolt implements IRichBolt {

    private TopologyContext context;
    private OutputCollector outputCollector;

    private Fields fields;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Parser parser = new Parser();
        Values emitValues = new Values();
        String msgBody = tuple.getStringByField("body");
        String type = tuple.getStringByField("type");

        System.out.println("[LOG] Parsing AMQP message..");
        emitValues.add(type);

        switch (type) {
            case "item":
                emitValues.add(parser.parseItem(msgBody));
                break;
        }

        System.out.println("[LOG] JSON transformed to Object.");

        outputCollector.emit(tuple, emitValues);
        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("item"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
