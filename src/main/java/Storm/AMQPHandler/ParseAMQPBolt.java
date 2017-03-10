package Storm.AMQPHandler;

import Storm.AMQPHandler.JSONObjects.*;
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
        String msgBody = tuple.getString(0);
        int index = msgBody.indexOf("extensionsXml");
        msgBody = msgBody.replaceAll("\\n \"extensionsXml.*,", "");

        log.debug(String.format("Parsing AMQP message %s..", tuple.getMessageId().toString()));

        switch (tuple.getSourceStreamId()) {
            case "item":
                Item item = parser.parseItem(msgBody);
                if (parser.validateItem(item) == null) {
                    emitValues.add(item);
                    _collector.emit(Streams.ITEM.id(), tuple, emitValues);
                } else {
                    _collector.emit(Streams.ERROR.id(), new Values("[ITEM]" + parser.validateItem(item)));
                }
                break;
            case "item-state":
                ItemState itemState = parser.parseItemState(msgBody);
                if (parser.validateItemState(itemState) == null) {
                    emitValues.add(itemState);
                    _collector.emit(Streams.ITEM_STATE.id(), tuple, emitValues);
                } else {
                    _collector.emit(Streams.ERROR.id(), new Values("[ITEM_STATE]" + parser.validateItemState(itemState)));
                }
                break;
            case "list":
                ListObj listObj = parser.parseList(msgBody);
                if (parser.validateList(listObj) == null) {
                    emitValues.add(listObj);
                    _collector.emit("list", tuple, emitValues);
                } else {
                    _collector.emit(Streams.ERROR.id(), new Values("[LIST]" + parser.validateList(listObj)));
                }
                break;
            case "list-state":
                break;
            case "drop":
                Drop drop = parser.parseDrop(msgBody);
                if (parser.validateDrop(drop) == null) {
                    emitValues.add(drop);
                    _collector.emit(Streams.DROP.id(), tuple, emitValues);
                } else {
                    _collector.emit(Streams.ERROR.id(), new Values("[DROP]" + parser.validateDrop(drop)));
                }
                break;
            case "drop-state":
                DropState dropState;
                break;;
        }
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.ITEM.id(), new Fields("item"));
        outputFieldsDeclarer.declareStream(Streams.ITEM_STATE.id(), new Fields("item-state"));
        outputFieldsDeclarer.declareStream(Streams.DROP.id(), new Fields("drop"));
        outputFieldsDeclarer.declareStream("list", new Fields("list"));

        outputFieldsDeclarer.declareStream(Streams.ERROR.id(), new Fields("error_msg"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
