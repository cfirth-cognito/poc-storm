package Storm.AMQPHandler;

import Storm.AMQPHandler.JSONObjects.Item;
import Storm.AMQPHandler.JSONObjects.ItemState;
import Storm.AMQPHandler.JSONObjects.ListObj;
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
public class ParseAMQPBolt implements IRichBolt {
    private static final Logger log = LoggerFactory.getLogger(ParseAMQPBolt.class);


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
        int index = msgBody.indexOf("extensionsXml");
        msgBody = msgBody.replaceAll("\\n \"extensionsXml.*,", "");

        log.debug(String.format("Parsing AMQP message %s..", tuple.getMessageId().toString()));

        switch (tuple.getSourceStreamId()) {
            case "item":
                Item item = parser.parseItem(msgBody);
                if (parser.validateItem(item) == null) {
                    emitValues.add(item);
                    _collector.emit("item", tuple, emitValues);
                } else {
                    _collector.emit("ErrorStream", new Values(parser.validateItem(item)));
                }
                break;
            case "item-state":
                ItemState itemState = parser.parseItemState(msgBody);
                if (parser.validateItemState(itemState) == null) {
                    emitValues.add(itemState);
                    _collector.emit("item-state", tuple, emitValues);
                } else {
                    _collector.emit("ErrorStream", new Values(parser.validateItemState(itemState)));
                }
                break;
            case "listObj":
                ListObj listObj = parser.parseList(msgBody);
                if (parser.validateList(listObj) == null) {
                    emitValues.add(listObj);
                    _collector.emit("listObj", tuple, emitValues);
                } else {
                    _collector.emit("ErrorStream", new Values(parser.validateList(listObj)));
                }
                break;
            case "listObj-state":
                break;
        }
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("item", new Fields("item"));
        outputFieldsDeclarer.declareStream("item-state", new Fields("item-state"));
        outputFieldsDeclarer.declareStream("list", new Fields("list"));
        outputFieldsDeclarer.declareStream("ErrorStream", new Fields("error_msg"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
