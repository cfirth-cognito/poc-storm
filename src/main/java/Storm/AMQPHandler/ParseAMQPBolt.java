package Storm.AMQPHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import Storm.AMQPHandler.JSONObj.Item.ItemState;
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
    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Parser parser = new Parser();
        Values emitValues = new Values();
        String msgBody = tuple.getStringByField("body");
        msgBody = msgBody.replaceAll("\"extensionsXml.*  ", "").replace("\\\"sla", "\"sla");

//        System.out.println(String.format("[LOG] Parsing AMQP message %s..", tuple.getMessageId().toString()));

        switch (tuple.getSourceStreamId()) {
            case "item":
                Item item = parser.parseItem(msgBody);
//                System.out.println("[LOG] Validation: " + parser.validateItem(item));
                if (parser.validateItem(item) == null) {
                    emitValues.add(item);
                    _collector.emit("item", tuple, emitValues);
                } else {
                    System.out.println("Item validation failed.");
                    _collector.emit("ErrorStream", new Values(parser.validateItem(item)));
                }
                break;
            case "item-state":
                ItemState itemState = parser.parseItemState(msgBody);
                if (parser.validateItemState(itemState) == null) {
                    emitValues.add(itemState);
                    _collector.emit("item-state", tuple, emitValues);
                } else {
                    System.out.println("Item State validation failed.");
                    _collector.emit("ErrorStream", new Values(parser.validateItemState(itemState)));
                }
                break;
            case "list":
                break;
            case "list-state":
                break;
        }
//        System.out.println("[LOG] JSON transformed to Object.");
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("item", new Fields("item"));
        outputFieldsDeclarer.declareStream("item-state", new Fields("item-state"));
        outputFieldsDeclarer.declareStream("ErrorStream", new Fields("error_msg"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
